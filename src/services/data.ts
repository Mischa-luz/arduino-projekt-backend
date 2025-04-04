import type { Service } from ".";

type DataKV = {
	timestamp: number;
	temperature: number;
	humidity: number;
	deviceId?: string;
};

type PostDataPayload = DataKV;
type GetDataResponse = DataKV[];

// Define supported time scales for data aggregation
type TimeScale = "raw" | "minute" | "hour" | "day" | "week" | "month";

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
				const scale = (url.searchParams.get("scale") as TimeScale) || "raw";
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

				const scaledData: GetDataResponse =
					scale === "raw" ? readings.slice(0, limit) : aggregateReadings(readings, scale, limit);

				return new Response(JSON.stringify(scaledData), {
					headers: { "Content-Type": "application/json" },
					status: 200,
				});
			}

			case "POST /": {
				try {
					const data: PostDataPayload = await request.json();

					if (typeof data.temperature !== "number" || typeof data.humidity !== "number") {
						return new Response("Missing or invalid temperature or humidity value", {
							status: 400,
						});
					}

					if (!data.timestamp) {
						data.timestamp = Date.now();
					}

					const key = `data_${data.timestamp}_${data.deviceId || "default"}`;

					await env.ARDUINO_DATA_KV.put(key, JSON.stringify(data));

					return new Response(JSON.stringify({ success: true, key }), {
						headers: { "Content-Type": "application/json" },
						status: 201,
					});
				} catch (error) {
					return new Response(`Error processing data: ${error}`, {
						status: 400,
					});
				}
			}
		}
	},
};

/**
 * Aggregates temperature readings based on the specified time scale
 */
function aggregateReadings(readings: DataKV[], scale: TimeScale, limit: number): DataKV[] {
	// If no readings or invalid scale, return empty array
	if (readings.length === 0 || scale === "raw") {
		return [];
	}

	const now = Date.now();
	const aggregated: DataKV[] = [];
	const buckets = new Map<number, { temperature: number[]; humidity: number[] }>();

	// Define time bucket size in milliseconds
	const bucketSize =
		scale === "minute"
			? 60000
			: scale === "hour"
				? 3600000
				: scale === "day"
					? 86400000
					: scale === "week"
						? 604800000
						: scale === "month"
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
