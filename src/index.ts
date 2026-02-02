export interface Env {
  GIT_R2: R2Bucket;
}

/* ======================================================
 * 基础工具
 * ====================================================== */

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function pktLine(data: string | Uint8Array): Uint8Array {
  const payload = typeof data === "string" ? encoder.encode(data) : data;
  const len = payload.length + 4;
  const header = encoder.encode(len.toString(16).padStart(4, "0"));
  const out = new Uint8Array(len);
  out.set(header, 0);
  out.set(payload, 4);
  return out;
}

function pktFlush(): Uint8Array {
  return encoder.encode("0000");
}

function concat(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((s, c) => s + c.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.length;
  }
  return out;
}

function zeroOid() {
  return "0000000000000000000000000000000000000000";
}

function join(...p: string[]) {
  return p.join("/");
}

/* ======================================================
 * 路径解析
 * ====================================================== */

function parseRepoPath(path: string) {
  const parts = path.split("/").filter(Boolean);
  if (parts.length < 2) return null;
  return {
    org: parts[0],
    repo: parts[1], // xxx.git
    subpath: parts.slice(2).join("/")
  };
}

/* ======================================================
 * R2 仓库操作
 * ====================================================== */

async function repoExists(env: Env, org: string, repo: string) {
  const list = await env.GIT_R2.list({
    prefix: join("repos", org, repo),
    limit: 1
  });
  return list.objects.length > 0;
}

async function initRepo(env: Env, org: string, repo: string) {
  const base = join("repos", org, repo);
  await env.GIT_R2.put(join(base, "HEAD"), "ref: refs/heads/main\n");
  await env.GIT_R2.put(join(base, "refs/heads/main"), "");
}

async function listRefs(env: Env, org: string, repo: string) {
  const prefix = join("repos", org, repo, "refs/heads/");
  const list = await env.GIT_R2.list({ prefix });
  const refs: Record<string, string> = {};

  for (const obj of list.objects) {
    const name = obj.key.slice(prefix.length);
    const data = await env.GIT_R2.get(obj.key);
    const sha = (await data!.text()).trim();
    if (sha) refs[`refs/heads/${name}`] = sha;
  }
  return refs;
}

/* ======================================================
 * info/refs
 * ====================================================== */

async function handleInfoRefs(
  env: Env,
  org: string,
  repo: string,
  service: string
) {
  const exists = await repoExists(env, org, repo);

  // 🔑 关键修复：push 允许自动建仓
  if (!exists && service === "git-receive-pack") {
    await initRepo(env, org, repo);
  }

  if (!exists && service === "git-upload-pack") {
    return new Response("Repository not found", { status: 404 });
  }

  const refs = await listRefs(env, org, repo);

  const capabilities =
    service === "git-receive-pack"
      ? "report-status delete-refs side-band-64k quiet atomic ofs-delta"
      : "multi_ack thin-pack side-band side-band-64k ofs-delta shallow";

  const out: Uint8Array[] = [];
  out.push(pktLine(`# service=${service}\n`));
  out.push(pktFlush());

  let first = true;
  for (const [ref, sha] of Object.entries(refs)) {
    if (first) {
      out.push(pktLine(`${sha || zeroOid()} ${ref}\0${capabilities}\n`));
      first = false;
    } else {
      out.push(pktLine(`${sha} ${ref}\n`));
    }
  }

  out.push(pktFlush());

  return new Response(concat(out), {
    headers: {
      "Content-Type": `application/x-${service}-advertisement`
    }
  });
}

/* ======================================================
 * git-upload-pack (clone)
 * ====================================================== */

async function handleUploadPack(env: Env, org: string, repo: string) {
  const prefix = join("repos", org, repo, "objects/pack/");
  const list = await env.GIT_R2.list({ prefix });

  if (list.objects.length === 0) {
    return new Response(pktFlush(), {
      headers: { "Content-Type": "application/x-git-upload-pack-result" }
    });
  }

  const latest = list.objects.sort((a, b) => b.uploaded - a.uploaded)[0];
  const pack = await env.GIT_R2.get(latest.key);

  const sideband = new Uint8Array([
    1,
    ...(new Uint8Array(await pack!.arrayBuffer()))
  ]);

  return new Response(concat([pktLine(sideband), pktFlush()]), {
    headers: { "Content-Type": "application/x-git-upload-pack-result" }
  });
}

/* ======================================================
 * git-receive-pack (push)
 * ====================================================== */

async function handleReceivePack(
  env: Env,
  org: string,
  repo: string,
  request: Request
) {
  const raw = new Uint8Array(await request.arrayBuffer());

  const packStart = findPackStart(raw);
  const pack = raw.slice(packStart);

  const hash = crypto.randomUUID().replace(/-/g, "");
  const base = join("repos", org, repo);

  await env.GIT_R2.put(
    join(base, `objects/pack/pack-${hash}.pack`),
    pack
  );

  const newSha = extractNewHead(raw);
  if (newSha) {
    await env.GIT_R2.put(
      join(base, "refs/heads/main"),
      newSha + "\n"
    );
  }

  const out = concat([
    pktLine("unpack ok\n"),
    pktLine("ok refs/heads/main\n"),
    pktFlush()
  ]);

  return new Response(out, {
    headers: {
      "Content-Type": "application/x-git-receive-pack-result"
    }
  });
}

/* ======================================================
 * pack / ref 解析（最小）
 * ====================================================== */

function findPackStart(buf: Uint8Array) {
  for (let i = 0; i < buf.length - 4; i++) {
    if (
      buf[i] === 0x50 && // P
      buf[i + 1] === 0x41 && // A
      buf[i + 2] === 0x43 && // C
      buf[i + 3] === 0x4b // K
    ) {
      return i;
    }
  }
  return 0;
}

function extractNewHead(buf: Uint8Array): string | null {
  const text = decoder.decode(buf);
  const m = text.match(
    /0000000000000000000000000000000000000000 ([0-9a-f]{40})/
  );
  return m ? m[1] : null;
}

/* ======================================================
 * Worker 入口
 * ====================================================== */

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const repo = parseRepoPath(url.pathname);
    if (!repo) return new Response("Bad path", { status: 400 });

    const { org, repo: repoName, subpath } = repo;

    if (subpath === "info/refs") {
      const service = url.searchParams.get("service");
      if (!service) return new Response("Missing service", { status: 400 });
      return handleInfoRefs(env, org, repoName, service);
    }

    if (subpath === "git-upload-pack" && request.method === "POST") {
      return handleUploadPack(env, org, repoName);
    }

    if (subpath === "git-receive-pack" && request.method === "POST") {
      return handleReceivePack(env, org, repoName, request);
    }

    return new Response("Not found", { status: 404 });
  }
};
