import type { Service } from ".";
import { z } from "zod";

type DataKV = {
	timestamp: number;
	temperature: number;
	humidity: number;
	deviceId?: string;
};

type GetDataResponse = DataKV[];

// Define supported time scales for data aggregation
export type TimeScale = "raw" | "30s" | "1m" | "5m" | "1h" | "6h" | "24h" | "7d" | "30d";

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
				const scale = (url.searchParams.get("timeScale") as TimeScale) || "raw";
				const limit = Number.parseInt(url.searchParams.get("limit") || "1000");

				const kvData = await env.ARDUINO_DATA_KV.list();

				const readings: DataKV[] = [];
				for (const key of kvData.keys) {
					const value: DataKV | null = await env.ARDUINO_DATA_KV.get(key.name, "json");
					if (value) {
						readings.push(value);
					}
				}

				readings.sort((a, b) => b.timestamp - a.timestamp);

				const scaledData: GetDataResponse = aggregateReadings(readings, scale, limit);

				return new Response(JSON.stringify(scaledData), {
					headers: { "Content-Type": "application/json" },
					status: 200,
				});
			}

			case "POST /": {
				let data: {
					timestamp?: number;
					temperature?: number;
					humidity?: number;
					deviceId?: string;
				} = {};

				switch (request.headers.get("Content-Type")) {
					case "application/json": {
						const { timestamp, humidity, temperature, deviceId } = await request.json<DataKV>();
						data = {
							timestamp: z.number().safeParse(timestamp).data ?? Date.now(),
							humidity: z.number().safeParse(humidity).data,
							temperature: z.number().safeParse(temperature).data,
							deviceId: z.string().safeParse(deviceId).data,
						};
						break;
					}
					case "application/x-www-form-urlencoded": {
						const form = await request.formData();

						data = {
							timestamp: z.number().safeParse(form.get("timestamp")).data ?? Date.now(),
							humidity: z.number().safeParse(form.get("humidity")).data,
							temperature: z.number().safeParse(form.get("temperature")).data,
							deviceId: z.string().safeParse(form.get("deviceId")).data,
						};
					}
				}

				if (typeof data.temperature !== "number" || typeof data.humidity !== "number") {
					return new Response("Missing or invalid temperature or humidity value", {
						status: 400,
					});
				}

				const key = `data_${data.timestamp}_${data.deviceId || "default"}`;

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
 * Aggregates temperature readings based on the specified time scale
 */
function aggregateReadings(readings: DataKV[], scale: TimeScale, limit: number): DataKV[] {
	// If no readings or invalid scale, return empty array

	const aggregated: DataKV[] = [];
	const buckets = new Map<number, { temperature: number[]; humidity: number[] }>();

	// Define time bucket size in milliseconds
	const bucketSize =
		scale === "raw"
			? 1
			: scale === "30s"
				? 30000
				: scale === "1m"
					? 60000
					: scale === "5m"
						? 300000
						: scale === "1h"
							? 3600000
							: scale === "6h"
								? 21600000
								: scale === "24h"
									? 86400000
									: scale === "7d"
										? 604800000
										: scale === "30d"
											? 2592000000
											: 3600000; // default to 1 hour

	// Group readings into time buckets
	for (const reading of readings) {
		// Calculate bucket key (timestamp rounded to bucket size)
		const bucketKey = Math.floor(reading.timestamp / bucketSize) * bucketSize;

		if (!buckets.has(bucketKey)) {
			buckets.set(bucketKey, { temperature: [], humidity: [] });
		}

		buckets.get(bucketKey)?.temperature.push(reading.temperature);
		buckets.get(bucketKey)?.humidity.push(reading.humidity);
	}

	// Calculate averages for each bucket
	for (const [timestamp, values] of buckets.entries()) {
		const temperatureAvg =
			values.temperature.reduce((sum, val) => sum + val, 0) / values.temperature.length;
		const humidityAvg = values.humidity.reduce((sum, val) => sum + val, 0) / values.humidity.length;

		aggregated.push({
			timestamp,
			temperature: Number(temperatureAvg.toFixed(2)),
			humidity: Number(humidityAvg.toFixed(2)),
		});
	}

	// Sort aggregated data by timestamp (newest first)
	aggregated.sort((a, b) => b.timestamp - a.timestamp);

	// Return limited number of readings
	return aggregated.slice(0, limit);
}
