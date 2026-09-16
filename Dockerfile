FROM oven/bun:1.3.7-debian AS dependencies

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

FROM oven/bun:1.3.7-debian AS runtime

ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000 \
    STAGING_ROOT=/var/lib/osimi/staging

WORKDIR /app
RUN mkdir -p /var/lib/osimi/staging \
    && chown -R 10001:10001 /var/lib/osimi /app

COPY --from=dependencies --chown=10001:10001 /app/node_modules ./node_modules
COPY --chown=10001:10001 package.json bun.lock index.ts ./
COPY --chown=10001:10001 src ./src
COPY --chown=10001:10001 deploy/docker-entrypoint.sh /usr/local/bin/osimi-entrypoint
RUN chmod 0555 /usr/local/bin/osimi-entrypoint

USER 10001:10001
EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/osimi-entrypoint"]
CMD ["bun", "run", "index.ts"]
