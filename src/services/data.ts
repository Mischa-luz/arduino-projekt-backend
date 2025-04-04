import type { Service } from ".";
import { z } from "zod";

type DataKV = {
	timestamp: number;
	temperature: number;
	humidity: number;
	deviceId?: string;
};

type GetDataResponse = DataKV[];

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export function enumToZod(myEnum: any): [string, ...string[]] {
	// biome-ignore lint/suspicious/noExplicitAny: <explanation>
	return Object.values(myEnum).map((value: any) => `${value}`) as [string, ...string[]];
}

export const TimeScale = {
	"30m": "30m",
	"1h": "1h",
	"6h": "6h",
	"24h": "24h",
	"7d": "7d",
	"30d": "30d",
	all: "all",
};
export type TimeScale = (typeof TimeScale)[keyof typeof TimeScale];

export const service: Service = {
	path: "/v1/data/",

	fetch: async (
		request: Request,
		env: Env,
		ctx: ExecutionContext,
		subPath: string,
	): Promise<Response | undefined> => {
		switch (`${request.method} /${subPath.split("/")[0]}`) {
			case "GET /": {
				const url = new URL(request.url);
				const timeScale =
					z.enum(enumToZod(TimeScale)).safeParse(url.searchParams.get("timeScale")).data ?? "24h";
				const limit = z.number().safeParse(url.searchParams.get("limit")).data ?? 1000;

				// Calculate cutoff time for the requested time window
				const now = Date.now();
				const timeWindowMs = getTimeWindowMs(timeScale);
				let cutoffTime = 0; // Default to earliest time (all data)

				// Set cutoff time for everything except "all"
				if (timeScale !== "all") {
					cutoffTime = now - timeWindowMs;
				}

				const collectedKeys: KVNamespaceListKey<unknown>[] = [];
				let cursor = undefined;
				while (collectedKeys.length < limit) {
					const listResult: KVNamespaceListResult<unknown> = await env.ARDUINO_DATA_KV.list({
						cursor,
					});
					const keys = listResult.keys;

					const filteredKeys = keys.filter((key) => {
						const timeStampString = key.name.split("_")[1];
						const timeStamp = z.coerce.number().safeParse(timeStampString).data;

						return timeStamp && timeStamp >= cutoffTime;
					});

					collectedKeys.push(...filteredKeys);

					if (listResult.list_complete) {
						break;
					}
					cursor = listResult.cursor;
				}

				const readings: DataKV[] = [];
				for (const key of collectedKeys) {
					const value: DataKV | null = await env.ARDUINO_DATA_KV.get(key.name, "json");
					if (value) {
						readings.push(value);
					}
				}

				readings.sort((a, b) => b.timestamp - a.timestamp);

				// Now that we have already filtered data by timestamp at the KV level,
				// we just need to handle aggregation
				const aggregationLevel = determineAggregationLevel(readings.length, timeWindowMs, limit);
				const filteredAndAggregatedData: GetDataResponse = aggregateReadings(
					readings,
					aggregationLevel,
					limit,
				);

				return new Response(JSON.stringify(filteredAndAggregatedData), {
					headers: { "Content-Type": "application/json" },
					status: 200,
				});
			}

			case "POST /": {
				let payload: {
					temperature?: number;
					humidity?: number;
					deviceId?: string;
				} = {};

				const contentType = request.headers.get("Content-Type");
				switch (contentType) {
					case "application/json": {
						const { humidity, temperature, deviceId } = await request.json<DataKV>();
						payload = {
							humidity: z.coerce.number().safeParse(humidity).data,
							temperature: z.coerce.number().safeParse(temperature).data,
							deviceId: z.string().safeParse(deviceId).data,
						};
						break;
					}
					case "application/x-www-form-urlencoded": {
						const form = await request.formData();

						payload = {
							humidity: z.coerce.number().safeParse(form.get("humidity")).data,
							temperature: z.coerce.number().safeParse(form.get("temperature")).data,
							deviceId: z.string().safeParse(form.get("deviceId")).data,
						};
						break;
					}
					default: {
						const text = await request.text();
						//parse temperature=value&humidity=value
						const params = new URLSearchParams(text);
						console.log("test: ", text);
						console.log("params: ", params);
						payload = {
							humidity: z.coerce.number().safeParse(params.get("humidity")).data,
							temperature: z.coerce.number().safeParse(params.get("temperature")).data,
							deviceId: z.string().safeParse(params.get("deviceId")).data,
						};
						break;
					}
				}

				console.log("Received data:", payload);

				if (typeof payload.temperature !== "number" || typeof payload.humidity !== "number") {
					return new Response("Missing or invalid temperature or humidity value", {
						status: 400,
					});
				}

				const key = `data_${Date.now()}_${payload.deviceId || "default"}`;
				const data: DataKV = {
					timestamp: Date.now(),
					temperature: payload.temperature,
					humidity: payload.humidity,
					deviceId: payload.deviceId,
				};

				await env.ARDUINO_DATA_KV.put(key, JSON.stringify(data), { expirationTtl: 86400 });

				return new Response(JSON.stringify({ success: true, key }), {
					headers: { "Content-Type": "application/json" },
					status: 201,
				});
			}
		}
	},
};

/**
 * Converts a TimeScale to milliseconds
 */
function getTimeWindowMs(timeScale: TimeScale): number {
	switch (timeScale) {
		case "30m":
			return 30 * 60 * 1000;
		case "1h":
			return 60 * 60 * 1000;
		case "6h":
			return 6 * 60 * 60 * 1000;
		case "24h":
			return 24 * 60 * 60 * 1000;
		case "7d":
			return 7 * 24 * 60 * 60 * 1000;
		case "30d":
			return 30 * 24 * 60 * 60 * 1000;
		case "all":
			return Number.POSITIVE_INFINITY;
		default:
			return 24 * 60 * 60 * 1000; // Default to 24h
	}
}

/**
 * Determines appropriate bucket size for aggregation based on data density
 */
function determineAggregationLevel(dataCount: number, timeWindowMs: number, limit: number): number {
	if (dataCount <= limit) {
		return 1;
	}

	const reductionFactor = Math.ceil(dataCount / limit);

	const minBucketSize = 15 * 1000; // 15 seconds
	const estimatedBucketSize = Math.ceil((timeWindowMs / limit) * reductionFactor);

	return Math.max(minBucketSize, estimatedBucketSize);
}

/**
 * Aggregates temperature readings based on the specified bucket size in milliseconds
 */
function aggregateReadings(readings: DataKV[], bucketSizeMs: number, limit: number): DataKV[] {
	// If no readings or invalid scale, return empty array
	if (!readings.length || bucketSizeMs <= 0) return [];

	const aggregated: DataKV[] = [];
	const buckets = new Map<
		number,
		{ temperature: number[]; humidity: number[]; deviceIds: Set<string> }
	>();

	// Group readings into time buckets
	for (const reading of readings) {
		// Calculate bucket key (timestamp rounded to bucket size)
		const bucketKey = Math.floor(reading.timestamp / bucketSizeMs) * bucketSizeMs;

		if (!buckets.has(bucketKey)) {
			buckets.set(bucketKey, { temperature: [], humidity: [], deviceIds: new Set() });
		}

		// biome-ignore lint/style/noNonNullAssertion: <explanation>
		const bucket = buckets.get(bucketKey)!;
		bucket.temperature.push(reading.temperature);
		bucket.humidity.push(reading.humidity);
		if (reading.deviceId) {
			bucket.deviceIds.add(reading.deviceId);
		}
	}

	// Calculate averages for each bucket
	for (const [timestamp, values] of buckets.entries()) {
		const temperatureAvg =
			values.temperature.reduce((sum, val) => sum + val, 0) / values.temperature.length;
		const humidityAvg = values.humidity.reduce((sum, val) => sum + val, 0) / values.humidity.length;

		// Combine deviceIds if there are multiple in this bucket
		let deviceId: string | undefined = undefined;
		if (values.deviceIds.size === 1) {
			deviceId = Array.from(values.deviceIds)[0];
		} else if (values.deviceIds.size > 1) {
			deviceId = Array.from(values.deviceIds).join(",");
		}

		aggregated.push({
			timestamp,
			temperature: Number(temperatureAvg.toFixed(2)),
			humidity: Number(humidityAvg.toFixed(2)),
			deviceId,
		});
	}

	// Sort aggregated data by timestamp (newest first)
	aggregated.sort((a, b) => b.timestamp - a.timestamp);

	// Return limited number of readings
	return aggregated.slice(0, limit);
}
