# A Linux host for the harness: glibc the compatibility check can read, RLIMIT_DATA that is enforced,
# PR_SET_PDEATHSIG that works, and mount points for the artifact share and the task's ephemeral disk.
FROM node:20-bookworm-slim@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0

RUN apt-get update \
	&& apt-get install -y --no-install-recommends python3 ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /harness
ENV NPM_CONFIG_UPDATE_NOTIFIER=false
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY tsconfig.json README.md ./
COPY src ./src
COPY test ./test
COPY runner ./runner
COPY tools ./tools
COPY docker ./docker
RUN npm run build

# Same shape as production: non-root, with the artifact share, the cache and the socket directory as separate mounts.
RUN useradd -u 10001 -m harness \
	&& mkdir -p /opt/validator/artifacts /opt/validator/cache /opt/validator/run \
	&& chown -R harness:harness /opt/validator /harness
USER harness

ENV VH_ARTIFACTS=/opt/validator/artifacts \
	VH_CACHE=/opt/validator/cache \
	VH_RUN_DIR=/opt/validator/run \
	VH_CACHE_MAX_MB=64

ENTRYPOINT ["/harness/docker/entrypoint.sh"]
CMD ["test"]
