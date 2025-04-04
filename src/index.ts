import * as services from "./services";
import type { Service } from "./services";

export default {
	async fetch(request, env, ctx): Promise<Response> {
		try {
			if (request.method === "OPTIONS") {
				return new Response(null, {
					status: 204,
					headers: {
						"Access-Control-Allow-Origin": "*", // TODO: update in production
						"Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
						"Access-Control-Allow-Headers": "Content-Type, Authorization",
						"Access-Control-Max-Age": "86400",
					},
				});
			}

			const url = new URL(request.url);
			const servicePath = `/${url.pathname.split("/").slice(1, 3).join("/")}/`;
			const subPath = url.pathname.substring(servicePath.length);

			const foundService = Object.values(services).filter(
				(service: Service) => service.path === servicePath,
			)[0];

			if (foundService) {
				const serviceResponse = await foundService.fetch(
					request,
					env,
					ctx,
					subPath,
				);

				if (serviceResponse) {
					serviceResponse.headers.set("Access-Control-Allow-Origin", "*"); // TODO: update in production

					return serviceResponse;
				}
			}

			return new Response("Service not found", { status: 404 });
		} catch (err) {
			console.error(`Error on request ${request.url}`, err);
			return new Response("Internal Server Error", { status: 500 });
		}
	},
} satisfies ExportedHandler<Env>;
