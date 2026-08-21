import { requireRole } from "../auth/guards.ts";
import { ValidationError } from "../http/errors.ts";
import { jsonResponse } from "../http/response.ts";
import { parseJsonBody } from "../validation/common.ts";
import { withWorkerAuth } from "./middleware.ts";
import {
    parseAccessRequestIdParam,
    parseArchiveRequestIdParam,
    parseArchiveRequestListQuery,
    parseArtifactIdParam,
    parseCreateAccessRequestBody,
    parseCreateObjectDownloadRequestBody,
    parseCreateObjectResyncBody,
    parseObjectEditHistoryQuery,
    parseObjectDownloadRequestIdParam,
    parseObjectIdParam,
    parseObjectListQuery,
    parsePatchObjectMetadataBody,
    parsePutDocumentCurationBody,
    parseReplaceObjectTextManifestBody,
    parseSubmitObjectCurationBody,
    parseResolveAccessRequestBody,
    parseReplaceObjectAvailableFilesBody,
    parseWorkerCompleteArchiveRequestBody,
    parseWorkerFailArchiveRequestBody,
    parseWorkerLeaseArchiveRequestBody,
    parseWorkerCompleteObjectDownloadRequestBody,
    parseWorkerFailObjectDownloadRequestBody,
    parseWorkerPresignObjectArtifactUploadBody,
    parseUpdateAccessPolicyBody,
    parseUpsertAccessAssignmentBody,
    parseUserIdParam,
} from "../validation/object.ts";
import { parseLeaseTokenBody } from "../validation/lease.ts";
import { parseUploadTokenParam } from "../validation/ingestion.ts";
import {
    completeArchiveRequestByWorker,
    downloadCurationPublicationSource,
    failArchiveRequestByWorker,
    heartbeatArchiveRequestLease,
    leaseNextArchiveRequest,
    presignArchiveRequestArtifactByWorker,
    releaseArchiveRequestLeaseByToken,
} from "../services/archive-request-service.ts";
import {
    getObjectEditDetail,
    getObjectCurationPublicationForTenant,
    getObjectEditHistoryForTenant,
    patchObjectMetadataForTenant,
    putDocumentCurationForTenant,
    releaseObjectEditLockForTenant,
    submitObjectCurationForTenant,
} from "../services/object-edit-service.ts";
import { replaceObjectTextManifest } from "../services/object-text-manifest-service.ts";
import {
    completeObjectDownloadRequestByWorker,
    createObjectDownloadRequestForTenant,
    createObjectAccessRequestForTenant,
    deleteObjectAccessAssignmentForTenant,
    downloadObjectArtifactForTenant,
    failObjectDownloadRequestByWorker,
    getObjectDetail,
    heartbeatObjectDownloadRequestLease,
    leaseNextObjectDownloadRequest,
    listObjectAccessAssignmentsForTenant,
    listObjectAccessRequestsForTenant,
    listArchiveRequestsForTenant,
    listObjectAvailableFilesForTenant,
    listObjectArtifactsForTenant,
    listObjectDownloadRequestsForTenant,
    listObjectResyncRequestsForTenant,
    listObjectsForTenant,
    presignObjectArtifactUpload,
    releaseObjectDownloadRequestLeaseByToken,
    requestObjectResyncForTenant,
    resolveObjectAccessRequestForTenant,
    replaceObjectAvailableFilesSnapshot,
    uploadObjectArtifactBySignedToken,
    updateObjectAccessPolicyForTenant,
    upsertObjectAccessAssignmentForTenant,
    viewObjectArtifactForTenant,
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

const listArchiveRequestsRoute: RouteDefinition = {
    method: "GET",
    path: "/api/archive-requests",
    handler: async (request, context) => {
        const authenticated = requireRole(context, [
            "viewer",
            "archiver",
            "admin",
        ]);
        const url = new URL(request.url);
        const query = parseArchiveRequestListQuery(url);

        return jsonResponse(
            await listArchiveRequestsForTenant({
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

const getObjectEditRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/edit",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["archiver", "admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/edit$/,
                "object_id",
            ),
        );
        return jsonResponse(
            await getObjectEditDetail({
                auth: authenticated,
                objectId,
            }),
        );
    },
};

const patchObjectMetadataRoute: RouteDefinition = {
    method: "PATCH",
    path: "/api/objects/:object_id/metadata",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["archiver", "admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/metadata$/,
                "object_id",
            ),
        );
        const body = parsePatchObjectMetadataBody(await parseJsonBody(request));
        return jsonResponse(
            await patchObjectMetadataForTenant({
                auth: authenticated,
                objectId,
                body,
            }),
        );
    },
};

const getObjectCurationHistoryRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/curation/history",
    handler: async (request, context) => {
        const authenticated = requireRole(context, [
            "viewer",
            "archiver",
            "admin",
        ]);
        const url = new URL(request.url);
        const objectId = parseObjectIdParam(
            extractPathParam(
                url.pathname,
                /^\/api\/objects\/([^/]+)\/curation\/history$/,
                "object_id",
            ),
        );
        const query = parseObjectEditHistoryQuery(url);
        return jsonResponse(
            await getObjectEditHistoryForTenant({
                auth: authenticated,
                objectId,
                query,
            }),
        );
    },
};

const putObjectDocumentCurationRoute: RouteDefinition = {
    method: "PUT",
    path: "/api/objects/:object_id/curation/document",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["archiver", "admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/curation\/document$/,
                "object_id",
            ),
        );
        const body = parsePutDocumentCurationBody(await parseJsonBody(request));
        return jsonResponse(
            await putDocumentCurationForTenant({
                auth: authenticated,
                objectId,
                body,
            }),
        );
    },
};

const submitObjectCurationRoute: RouteDefinition = {
    method: "POST",
    path: "/api/objects/:object_id/curation/submit",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["archiver", "admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/curation\/submit$/,
                "object_id",
            ),
        );
        const body = parseSubmitObjectCurationBody(
            await parseJsonBody(request),
        );
        return jsonResponse(
            await submitObjectCurationForTenant({
                auth: authenticated,
                objectId,
                body,
            }),
        );
    },
};

const downloadCurationPublicationSourceRoute: RouteDefinition = {
    method: "GET",
    path: "/api/archive-requests/:request_id/source",
    handler: withWorkerAuth(async (request, _context, worker) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseArchiveRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/archive-requests\/([^/]+)\/source$/,
                "request_id",
            ),
        );
        const leaseToken = request.headers
            .get("x-archive-request-lease-token")
            ?.trim();
        if (!leaseToken) {
            throw new ValidationError(
                "Header 'x-archive-request-lease-token' is required.",
            );
        }
        return await downloadCurationPublicationSource({
            requestId,
            leaseToken,
            workerId: worker.workerId,
        });
    }),
};

const getObjectCurationPublicationRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/curation-publication",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["archiver", "admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/curation-publication$/,
                "object_id",
            ),
        );
        return jsonResponse(
            await getObjectCurationPublicationForTenant({
                auth: authenticated,
                objectId,
            }),
        );
    },
};

const deleteObjectEditLockRoute: RouteDefinition = {
    method: "DELETE",
    path: "/api/objects/:object_id/edit-lock",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["archiver", "admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/edit-lock$/,
                "object_id",
            ),
        );
        return jsonResponse(
            await releaseObjectEditLockForTenant({
                auth: authenticated,
                objectId,
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

const listObjectAvailableFilesRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/available-files",
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
                /^\/api\/objects\/([^/]+)\/available-files$/,
                "object_id",
            ),
        );

        return jsonResponse(
            await listObjectAvailableFilesForTenant({
                auth: authenticated,
                objectId,
            }),
        );
    },
};

const replaceObjectAvailableFilesRoute: RouteDefinition = {
    method: "PUT",
    path: "/api/internal/objects/:object_id/available-files",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/internal\/objects\/([^/]+)\/available-files$/,
                "object_id",
            ),
        );
        const body = parseReplaceObjectAvailableFilesBody(
            await parseJsonBody(request),
        );

        return jsonResponse(
            await replaceObjectAvailableFilesSnapshot({
                objectId,
                body,
            }),
        );
    }),
};

const replaceObjectTextManifestRoute: RouteDefinition = {
    method: "PUT",
    path: "/api/internal/objects/:object_id/object-text-manifest",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/internal\/objects\/([^/]+)\/object-text-manifest$/,
                "object_id",
            ),
        );
        const body = parseReplaceObjectTextManifestBody(
            await parseJsonBody(request),
        );

        return jsonResponse(
            await replaceObjectTextManifest({
                objectId,
                body,
            }),
        );
    }),
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

const requestObjectResyncRoute: RouteDefinition = {
    method: "POST",
    path: "/api/objects/:object_id/resync",
    handler: async (request, context) => {
        const authenticated = requireRole(context, ["archiver", "admin"]);
        const pathname = new URL(request.url).pathname;
        const objectId = parseObjectIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/([^/]+)\/resync$/,
                "object_id",
            ),
        );
        const body = parseCreateObjectResyncBody(
            await parseOptionalJsonBody(request),
        );

        const result = await requestObjectResyncForTenant({
            auth: authenticated,
            objectId,
            actionPayload: body.action_payload,
        });

        return jsonResponse(result.response, {
            status: result.outcome === "created" ? 201 : 200,
        });
    },
};

const listObjectResyncRequestsRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/resync-requests",
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
                /^\/api\/objects\/([^/]+)\/resync-requests$/,
                "object_id",
            ),
        );

        return jsonResponse(
            await listObjectResyncRequestsForTenant({
                auth: authenticated,
                objectId,
            }),
        );
    },
};

const leaseArchiveRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/archive-requests/lease",
    handler: withWorkerAuth(async (request, _context, worker) => {
        const body = parseWorkerLeaseArchiveRequestBody(
            await parseOptionalJsonBody(request),
        );

        return jsonResponse(
            await leaseNextArchiveRequest({
                workerId: worker.workerId,
                actionType: body.action_type,
            }),
        );
    }),
};

const heartbeatArchiveRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/archive-requests/:id/lease/heartbeat",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseArchiveRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/archive-requests\/([^/]+)\/lease\/heartbeat$/,
                "id",
            ),
        );
        const body = parseLeaseTokenBody(await parseJsonBody(request));

        return jsonResponse(
            await heartbeatArchiveRequestLease({
                requestId,
                leaseToken: body.lease_token,
            }),
        );
    }),
};

const releaseArchiveRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/archive-requests/:id/lease/release",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseArchiveRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/archive-requests\/([^/]+)\/lease\/release$/,
                "id",
            ),
        );
        const body = parseLeaseTokenBody(await parseJsonBody(request));

        return jsonResponse(
            await releaseArchiveRequestLeaseByToken({
                requestId,
                leaseToken: body.lease_token,
            }),
        );
    }),
};

const presignArchiveRequestArtifactRoute: RouteDefinition = {
    method: "POST",
    path: "/api/archive-requests/:id/artifacts/presign",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseArchiveRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/archive-requests\/([^/]+)\/artifacts\/presign$/,
                "id",
            ),
        );
        const body = parseWorkerPresignObjectArtifactUploadBody(
            await parseJsonBody(request),
        );

        return jsonResponse(
            await presignArchiveRequestArtifactByWorker({
                requestId,
                body,
            }),
        );
    }),
};

const completeArchiveRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/archive-requests/:id/complete",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseArchiveRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/archive-requests\/([^/]+)\/complete$/,
                "id",
            ),
        );
        const body = parseWorkerCompleteArchiveRequestBody(
            await parseJsonBody(request),
        );

        return jsonResponse(
            await completeArchiveRequestByWorker({
                requestId,
                body,
            }),
        );
    }),
};

const failArchiveRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/archive-requests/:id/fail",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseArchiveRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/archive-requests\/([^/]+)\/fail$/,
                "id",
            ),
        );
        const body = parseWorkerFailArchiveRequestBody(
            await parseJsonBody(request),
        );

        return jsonResponse(
            await failArchiveRequestByWorker({
                requestId,
                body,
            }),
        );
    }),
};

const leaseObjectDownloadRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/object-download-requests/lease",
    handler: withWorkerAuth(async (_request, _context, worker) => {
        return jsonResponse(
            await leaseNextObjectDownloadRequest({
                workerId: worker.workerId,
            }),
        );
    }),
};

const heartbeatObjectDownloadRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/object-download-requests/:id/lease/heartbeat",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseObjectDownloadRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/object-download-requests\/([^/]+)\/lease\/heartbeat$/,
                "id",
            ),
        );
        const body = parseLeaseTokenBody(await parseJsonBody(request));

        return jsonResponse(
            await heartbeatObjectDownloadRequestLease({
                requestId,
                leaseToken: body.lease_token,
            }),
        );
    }),
};

const releaseObjectDownloadRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/object-download-requests/:id/lease/release",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseObjectDownloadRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/object-download-requests\/([^/]+)\/lease\/release$/,
                "id",
            ),
        );
        const body = parseLeaseTokenBody(await parseJsonBody(request));

        return jsonResponse(
            await releaseObjectDownloadRequestLeaseByToken({
                requestId,
                leaseToken: body.lease_token,
            }),
        );
    }),
};

const presignObjectDownloadRequestArtifactRoute: RouteDefinition = {
    method: "POST",
    path: "/api/object-download-requests/:id/artifacts/presign",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseObjectDownloadRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/object-download-requests\/([^/]+)\/artifacts\/presign$/,
                "id",
            ),
        );
        const body = parseWorkerPresignObjectArtifactUploadBody(
            await parseJsonBody(request),
        );

        return jsonResponse(
            await presignObjectArtifactUpload({
                requestId,
                body,
            }),
        );
    }),
};

const completeObjectDownloadRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/object-download-requests/:id/complete",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseObjectDownloadRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/object-download-requests\/([^/]+)\/complete$/,
                "id",
            ),
        );
        const body = parseWorkerCompleteObjectDownloadRequestBody(
            await parseJsonBody(request),
        );

        return jsonResponse(
            await completeObjectDownloadRequestByWorker({
                requestId,
                body,
            }),
        );
    }),
};

const failObjectDownloadRequestRoute: RouteDefinition = {
    method: "POST",
    path: "/api/object-download-requests/:id/fail",
    handler: withWorkerAuth(async (request) => {
        const pathname = new URL(request.url).pathname;
        const requestId = parseObjectDownloadRequestIdParam(
            extractPathParam(
                pathname,
                /^\/api\/object-download-requests\/([^/]+)\/fail$/,
                "id",
            ),
        );
        const body = parseWorkerFailObjectDownloadRequestBody(
            await parseJsonBody(request),
        );

        return jsonResponse(
            await failObjectDownloadRequestByWorker({
                requestId,
                body,
            }),
        );
    }),
};

const workerUploadObjectArtifactRoute: RouteDefinition = {
    method: "PUT",
    path: "/api/archive-requests/uploads/:token",
    handler: async (request) => {
        const pathname = new URL(request.url).pathname;
        const uploadToken = parseUploadTokenParam(
            extractPathParam(
                pathname,
                /^\/api\/archive-requests\/uploads\/([^/]+)$/,
                "token",
            ),
        );

        return jsonResponse(
            await uploadObjectArtifactBySignedToken({
                uploadToken,
                request,
            }),
        );
    },
};

const workerUploadObjectArtifactLegacyRoute: RouteDefinition = {
    method: "PUT",
    path: "/api/object-download-requests/uploads/:token",
    handler: async (request) => {
        const pathname = new URL(request.url).pathname;
        const uploadToken = parseUploadTokenParam(
            extractPathParam(
                pathname,
                /^\/api\/object-download-requests\/uploads\/([^/]+)$/,
                "token",
            ),
        );

        return jsonResponse(
            await uploadObjectArtifactBySignedToken({
                uploadToken,
                request,
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

const viewArtifactRoute: RouteDefinition = {
    method: "GET",
    path: "/api/objects/:object_id/artifacts/:artifact_id/view",
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
                /^\/api\/objects\/([^/]+)\/artifacts\/[^/]+\/view$/,
                "object_id",
            ),
        );
        const artifactId = parseArtifactIdParam(
            extractPathParam(
                pathname,
                /^\/api\/objects\/[^/]+\/artifacts\/([^/]+)\/view$/,
                "artifact_id",
            ),
        );
        return viewObjectArtifactForTenant({
            auth: authenticated,
            objectId,
            artifactId,
            rangeHeader: request.headers.get("range"),
            ifRangeHeader: request.headers.get("if-range"),
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
    listArchiveRequestsRoute,
    getObjectRoute,
    getObjectEditRoute,
    patchObjectMetadataRoute,
    listArtifactsRoute,
    listObjectAvailableFilesRoute,
    getObjectCurationHistoryRoute,
    putObjectDocumentCurationRoute,
    submitObjectCurationRoute,
    getObjectCurationPublicationRoute,
    downloadCurationPublicationSourceRoute,
    deleteObjectEditLockRoute,
    requestObjectResyncRoute,
    listObjectResyncRequestsRoute,
    createObjectDownloadRequestRoute,
    listObjectDownloadRequestsRoute,
    leaseArchiveRequestRoute,
    heartbeatArchiveRequestRoute,
    releaseArchiveRequestRoute,
    presignArchiveRequestArtifactRoute,
    completeArchiveRequestRoute,
    failArchiveRequestRoute,
    leaseObjectDownloadRequestRoute,
    heartbeatObjectDownloadRequestRoute,
    releaseObjectDownloadRequestRoute,
    presignObjectDownloadRequestArtifactRoute,
    completeObjectDownloadRequestRoute,
    failObjectDownloadRequestRoute,
    workerUploadObjectArtifactRoute,
    workerUploadObjectArtifactLegacyRoute,
    replaceObjectAvailableFilesRoute,
    replaceObjectTextManifestRoute,
    downloadArtifactRoute,
    viewArtifactRoute,
    patchObjectAccessPolicyRoute,
    createObjectAccessRequestRoute,
    listObjectAccessRequestsRoute,
    approveObjectAccessRequestRoute,
    rejectObjectAccessRequestRoute,
    listObjectAccessAssignmentsRoute,
    upsertObjectAccessAssignmentRoute,
    deleteObjectAccessAssignmentRoute,
];
