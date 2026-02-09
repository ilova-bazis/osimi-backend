export function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  const response = new Response(JSON.stringify(payload), init);

  if (!response.headers.has("content-type")) {
    response.headers.set("content-type", "application/json; charset=utf-8");
  }

  return response;
}
