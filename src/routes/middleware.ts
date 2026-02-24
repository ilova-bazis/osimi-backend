import {
  authorizeWorkerLeaseForIngestion,
  type AuthorizedWorkerLease,
} from "../auth/worker-lease.ts";
import type { WorkerPrincipal } from "../auth/worker.ts";
import { requireWorkerAuthentication } from "../auth/worker.ts";
import type { RequestContext } from "../http/context.ts";
import { parseJsonBody } from "../validation/common.ts";

import { extractPathParam } from "./params.ts";
import type { RouteDefinition } from "./types.ts";

type WorkerRouteHandler = (
  request: Request,
  context: RequestContext,
  worker: WorkerPrincipal,
) => Response | Promise<Response>;

export function withWorkerAuth(
  handler: WorkerRouteHandler,
): RouteDefinition["handler"] {
  return (request, context) => {
    const worker = requireWorkerAuthentication(request);
    return handler(request, context, worker);
  };
}

interface WorkerIngestionBodyRouteData<TBody> {
  worker: WorkerPrincipal;
  ingestionId: string;
  body: TBody;
}

type WorkerIngestionBodyRouteHandler<TBody> = (
  request: Request,
  context: RequestContext,
  data: WorkerIngestionBodyRouteData<TBody>,
) => Response | Promise<Response>;

export function withWorkerIngestionJsonBody<TBody>(params: {
  pathPattern: RegExp;
  pathParamName: string;
  parseBody: (body: unknown) => TBody;
  parseParam?: (value: string) => string;
  handler: WorkerIngestionBodyRouteHandler<TBody>;
}): RouteDefinition["handler"] {
  return withWorkerAuth(async (request, context, worker) => {
    const pathname = new URL(request.url).pathname;
    const ingestionIdRaw = extractPathParam(
      pathname,
      params.pathPattern,
      params.pathParamName,
    );
    const ingestionId = params.parseParam
      ? params.parseParam(ingestionIdRaw)
      : ingestionIdRaw;
    const rawBody = await parseJsonBody(request);
    const body = params.parseBody(rawBody);

    return params.handler(request, context, {
      worker,
      ingestionId,
      body,
    });
  });
}

interface WorkerAuthorizedLeaseRouteData<TBody extends { lease_token: string }>
  extends WorkerIngestionBodyRouteData<TBody> {
  authorizedLease: AuthorizedWorkerLease;
}

type WorkerAuthorizedLeaseRouteHandler<TBody extends { lease_token: string }> = (
  request: Request,
  context: RequestContext,
  data: WorkerAuthorizedLeaseRouteData<TBody>,
) => Response | Promise<Response>;

export function withWorkerAuthorizedLease<TBody extends { lease_token: string }>(
  params: {
    pathPattern: RegExp;
    pathParamName: string;
    parseBody: (body: unknown) => TBody;
    parseParam?: (value: string) => string;
    handler: WorkerAuthorizedLeaseRouteHandler<TBody>;
  },
): RouteDefinition["handler"] {
  return withWorkerIngestionJsonBody({
    pathPattern: params.pathPattern,
    pathParamName: params.pathParamName,
    parseBody: params.parseBody,
    parseParam: params.parseParam,
    handler: async (request, context, data) => {
      const authorizedLease = await authorizeWorkerLeaseForIngestion({
        ingestionId: data.ingestionId,
        leaseToken: data.body.lease_token,
      });

      return params.handler(request, context, {
        ...data,
        authorizedLease,
      });
    },
  });
}
