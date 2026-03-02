import { requireRole } from "../auth/guards.ts";
import { ValidationError } from "../http/errors.ts";
import { jsonResponse } from "../http/response.ts";
import { parseJsonBody } from "../validation/common.ts";
import {
    parseAccessRequestIdParam,
    parseArtifactIdParam,
    parseCreateAccessRequestBody,
    parseCreateObjectDownloadRequestBody,
    parseObjectIdParam,
    parseObjectListQuery,
    parsePatchObjectTitleBody,
    parseResolveAccessRequestBody,
    parseUpdateAccessPolicyBody,
    parseUpsertAccessAssignmentBody,
    parseUserIdParam,
} from "../validation/object.ts";
import {
    createObjectDownloadRequestForTenant,
    createObjectAccessRequestForTenant,
    deleteObjectAccessAssignmentForTenant,
    downloadObjectArtifactForTenant,
    getObjectDetail,
    listObjectAccessAssignmentsForTenant,
    listObjectAccessRequestsForTenant,
    listObjectArtifactsForTenant,
    listObjectDownloadRequestsForTenant,
    listObjectsForTenant,
    patchObjectTitleForTenant,
    resolveObjectAccessRequestForTenant,
    updateObjectAccessPolicyForTenant,
    upsertObjectAccessAssignmentForTenant,
} from "../services/object-service.ts";
import { extractPathParam } from "./params.ts";
import type { RouteDefinition } from "./types.ts";

async function parseOptionalJsonBody(request: Request): Promise<unknown> {
    const rawBody = await request.text();
    if (rawBody.trim().length === 0) {
        return {};
    }

    try {
        return JSON.parse(rawBody) as unknown;
    } catch {
        throw new ValidationError("Request body must be valid JSON.");
    }
}

const listObjectsRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects",
    handler: async (request, context) => {
        const authenticated = requireRole(context, [
            "viewer",
            "archiver",
            "admin",
        ]);
        const url = new URL(request.url);
        const query = parseObjectListQuery(url);
        return jsonResponse(
            await listObjectsForTenant({
                auth: authenticated,
                query,
            }),
        );
    },
};

const getObjectRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id",
    handler: async (request, context) => {
        const authenticated = requireRole(context, [
            "viewer",
            "archiver",
            "admin",
        ]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)$/,
                "object_id",
            ),
        );
        return jsonResponse(
            await getObjectDetail({
                auth: authenticated,
                objectId,
            }),
        );
    },
};

const patchObjectRoute: RouteDefinition = {
    method: "PATCH",
    path: "/api/objects/:object_id",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["archiver", "admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)$/,
                "object_id",
            ),
        );
        const body = parsePatchObjectTitleBody(await parseJsonBody(request));
        return jsonResponse(
            await patchObjectTitleForTenant({
                auth: authenticated,
                objectId,
                body,
            }),
        );
    },
};

const listArtifactsRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/artifacts",
    handler: async (request, context) => {
        const authenticated = requireRole(context, [
            "viewer",
            "archiver",
            "admin",
        ]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/artifacts$/,
                "object_id",
            ),
        );
        return jsonResponse(
            await listObjectArtifactsForTenant({
                auth: authenticated,
                objectId,
            }),
        );
    },
};

const createObjectDownloadRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/objects/:object_id/download-requests",
    handler: async (request, context) => {
        const authenticated = requireRole(context, [
            "viewer",
            "archiver",
            "admin",
        ]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/download-requests$/,
                "object_id",
            ),
        );
        const body = parseCreateObjectDownloadRequestBody(
            await parseJsonBody(request),
        );

        const result = await createObjectDownloadRequestForTenant({
            auth: authenticated,
            objectId,
            body,
        });

        return jsonResponse(result.response, {
            status: result.outcome === "created" ? 201 : 200,
        });
    },
};

const listObjectDownloadRequestsRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/download-requests",
    handler: async (request, context) => {
        const authenticated = requireRole(context, [
            "viewer",
            "archiver",
            "admin",
        ]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/download-requests$/,
                "object_id",
            ),
        );

        return jsonResponse(
            await listObjectDownloadRequestsForTenant({
                auth: authenticated,
                objectId,
            }),
        );
    },
};

const downloadArtifactRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/artifacts/:artifact_id/download",
    handler: async (request, context) => {
        const authenticated = requireRole(context, [
            "viewer",
            "archiver",
            "admin",
        ]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/artifacts\/[^/]+\/download$/,
                "object_id",
            ),
        );
        const extractedArtifactId = extractPathParam(
            pathname,
            /^\/api\/objects\/[^/]+\/artifacts\/([^/]+)\/download$/,
            "artifact_id",
        );
        const artifactId = parseArtifactIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/[^/]+\/artifacts\/([^/]+)\/download$/,
                "artifact_id",
            ),
        );
        return downloadObjectArtifactForTenant({
            auth: authenticated,
            objectId,
            artifactId,
        });
    },
};

const patchObjectAccessPolicyRoute: RouteDefinition = {
    method: "PATCH",
    path: "/api/objects/:object_id/access-policy",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/access-policy$/,
                "object_id",
            ),
        );
        const body = parseUpdateAccessPolicyBody(await parseJsonBody(request));
        return jsonResponse(
            await updateObjectAccessPolicyForTenant({
                auth: authenticated,
                objectId,
                body,
            }),
        );
    },
};

const createObjectAccessRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/objects/:object_id/access-requests",
    handler: async (request, context) => {
        const authenticated = requireRole(context, [
            "viewer",
            "archiver",
            "admin",
        ]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/access-requests$/,
                "object_id",
            ),
        );
        const body = parseCreateAccessRequestBody(await parseJsonBody(request));
        return jsonResponse(
            await createObjectAccessRequestForTenant({
                auth: authenticated,
                objectId,
                body,
            }),
            {
                status: 201,
            },
        );
    },
};

const listObjectAccessRequestsRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/access-requests",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/access-requests$/,
                "object_id",
            ),
        );
        return jsonResponse(
            await listObjectAccessRequestsForTenant({
                auth: authenticated,
                objectId,
            }),
        );
    },
};

const approveObjectAccessRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/objects/:object_id/access-requests/:request_id/approve",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/access-requests\/[^/]+\/approve$/,
                "object_id",
            ),
        );
        const requestId = parseAccessRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/[^/]+\/access-requests\/([^/]+)\/approve$/,
                "request_id",
            ),
        );
        const body = parseResolveAccessRequestBody(
            await parseOptionalJsonBody(request),
        );
        return jsonResponse(
            await resolveObjectAccessRequestForTenant({
                auth: authenticated,
                objectId,
                requestId,
                action: "approve",
                body,
            }),
        );
    },
};

const rejectObjectAccessRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/objects/:object_id/access-requests/:request_id/reject",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/access-requests\/[^/]+\/reject$/,
                "object_id",
            ),
        );
        const requestId = parseAccessRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/[^/]+\/access-requests\/([^/]+)\/reject$/,
                "request_id",
            ),
        );
        const body = parseResolveAccessRequestBody(
            await parseOptionalJsonBody(request),
        );
        return jsonResponse(
            await resolveObjectAccessRequestForTenant({
                auth: authenticated,
                objectId,
                requestId,
                action: "reject",
                body,
            }),
        );
    },
};

const listObjectAccessAssignmentsRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/access-assignments",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/access-assignments$/,
                "object_id",
            ),
        );
        return jsonResponse(
            await listObjectAccessAssignmentsForTenant({
                auth: authenticated,
                objectId,
            }),
        );
    },
};

const upsertObjectAccessAssignmentRoute: RouteDefinition = {
    method: "PUT",
    path: "/api/objects/:object_id/access-assignments",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/access-assignments$/,
                "object_id",
            ),
        );
        const body = parseUpsertAccessAssignmentBody(
            await parseJsonBody(request),
        );
        return jsonResponse(
            await upsertObjectAccessAssignmentForTenant({
                auth: authenticated,
                objectId,
                body,
            }),
        );
    },
};

const deleteObjectAccessAssignmentRoute: RouteDefinition = {
    method: "DELETE",
    path: "/api/objects/:object_id/access-assignments/:user_id",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/access-assignments\/[^/]+$/,
                "object_id",
            ),
        );
        const userId = parseUserIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/[^/]+\/access-assignments\/([^/]+)$/,
                "user_id",
            ),
        );
        return jsonResponse(
            await deleteObjectAccessAssignmentForTenant({
                auth: authenticated,
                objectId,
                userId,
            }),
        );
    },
};

export const objectRoutes: RouteDefinition[] = [
    listObjectsRoute,
    getObjectRoute,
    patchObjectRoute,
    listArtifactsRoute,
    createObjectDownloadRequestRoute,
    listObjectDownloadRequestsRoute,
    downloadArtifactRoute,
    patchObjectAccessPolicyRoute,
    createObjectAccessRequestRoute,
    listObjectAccessRequestsRoute,
    approveObjectAccessRequestRoute,
    rejectObjectAccessRequestRoute,
    listObjectAccessAssignmentsRoute,
    upsertObjectAccessAssignmentRoute,
    deleteObjectAccessAssignmentRoute,
];
