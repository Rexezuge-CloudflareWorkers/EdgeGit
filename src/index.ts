export interface Env {
  GIT_R2: R2Bucket;
}

/* ======================================================
 * 基础工具
 * ====================================================== */
function sidebandPacket(data: Uint8Array) {
  const out = new Uint8Array(data.length + 1);
  out[0] = 1; // channel 1
  out.set(data, 1);
  return pktLine(out);
}


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
    : "multi_ack_detailed multi_ack thin-pack side-band side-band-64k ofs-delta shallow no-progress include-tag";

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
  const packObj = await env.GIT_R2.get(latest.key);
  const packData = new Uint8Array(await packObj!.arrayBuffer());

  const chunks: Uint8Array[] = [];

  const MAX = 65500; // pkt safe

  for (let i = 0; i < packData.length; i += MAX) {
    const slice = packData.slice(i, i + MAX);

    const side = new Uint8Array(slice.length + 1);
    side[0] = 1; // channel 1 = pack
    side.set(slice, 1);

    chunks.push(pktLine(side));
  }

  chunks.push(pktFlush());

  return new Response(concat(chunks), {
    headers: {
      "Content-Type": "application/x-git-upload-pack-result"
    }
  });
}


/* ======================================================
 * git-receive-pack (push)
 * ====================================================== */

/* ======================================================
 * 修正版 initRepo
 * ====================================================== */
async function initRepo(env: Env, org: string, repo: string) {
  const base = join("repos", org, repo);
  await env.GIT_R2.put(join(base, "HEAD"), "ref: refs/heads/main\n");
  await env.GIT_R2.put(join(base, "refs/heads/main"), zeroOid() + "\n");
}

/* ======================================================
 * 修正版 readRef
 * ====================================================== */
async function readRef(env: Env, base: string, ref: string) {
  const obj = await env.GIT_R2.get(join(base, ref));
  if (!obj) return zeroOid();
  const text = (await obj.text()).trim();
  return text === "" ? zeroOid() : text;
}

/* ======================================================
 * 修正版 handleReceivePack
 * ====================================================== */
async function handleReceivePack(env: Env, org: string, repo: string, request: Request) {
  const buf = new Uint8Array(await request.arrayBuffer());
  const { commands, packStart } = parseReceiveCommands(buf);
  const base = join("repos", org, repo);

  const results: Uint8Array[] = [];
  results.push(pktLine("unpack ok\n"));

  for (const cmd of commands) {
    const current = await readRef(env, base, cmd.ref);
    const oldSha = cmd.old === zeroOid() ? zeroOid() : cmd.old;

    if (current !== oldSha && oldSha !== zeroOid()) {
      results.push(pktLine(`ng ${cmd.ref} non-fast-forward\n`));
      continue;
    }

    await env.GIT_R2.put(join(base, cmd.ref), cmd.new + "\n");
    results.push(pktLine(`ok ${cmd.ref}\n`));
  }

  // 存储 pack
  const pack = buf.slice(packStart);
  if (pack.length > 0) {
    const hash = crypto.randomUUID().replace(/-/g, "");
    await env.GIT_R2.put(join(base, `objects/pack/pack-${hash}.pack`), pack);
  }

  // side-band channel 1 包装每条 pktLine
  const sideband = concat(results.map(r => sidebandPacket(r)));
  const final = concat([sideband, pktFlush()]);

  return new Response(final, {
    headers: { "Content-Type": "application/x-git-receive-pack-result" }
  });
}


/* ======================================================
 * parseReceiveCommands 保持原有逻辑
 * ====================================================== */
function parseReceiveCommands(buf: Uint8Array) {
  let i = 0;
  const commands = [];

  while (true) {
    const len = parseInt(decoder.decode(buf.slice(i, i + 4)), 16);
    if (len === 0) {
      i += 4;
      break;
    }

    const line = decoder.decode(buf.slice(i + 4, i + len));
    i += len;

    const [oldSha, newSha, refPart] = line.trim().split(" ");
    const ref = refPart.split("\0")[0];

    commands.push({ old: oldSha, new: newSha, ref });
  }

  return { commands, packStart: findPackStart(buf) };
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
