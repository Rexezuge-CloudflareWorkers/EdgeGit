
interface Env {
  R2_BUCKET: R2Bucket;
}

// pkt-line format
function pktLine(str: string) {
  const hexLength = (str.length + 5).toString(16).padStart(4, '0');
  return `${hexLength}${str}\n`;
}

function flushPkt() {
  return '0000';
}

async function parsePktLine(reader: ReadableStreamDefaultReader) {
  let buffer = new Uint8Array();
  const decoder = new TextDecoder();

  async function read(bytes: number) {
    while (buffer.length < bytes) {
      const { done, value } = await reader.read();
      if (done) {
        return null;
      }
      buffer = new Uint8Array([...buffer, ...value]);
    }
    const result = buffer.slice(0, bytes);
    buffer = buffer.slice(bytes);
    return result;
  }

  const lengthHex = await read(4);
  if (lengthHex === null) {
    return null;
  }
  const length = parseInt(decoder.decode(lengthHex), 16);
  if (length === 0) {
    return null;
  }
  const data = await read(length - 4);
  if (data === null) {
    return null;
  }
  return decoder.decode(data);
}

const repoRegex = /^\/([a-zA-Z0-9_-]+)\/([a-zA-Z0-9_-]+)\.git\/(info\/refs|git-receive-pack)$/;

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const { pathname, searchParams } = url;
    const service = searchParams.get('service');
    const match = pathname.match(repoRegex);

    if (!match) {
      return new Response('Not found', { status: 404 });
    }

    const [_, org, repo, action] = match;
    const repoPrefix = `${org}/${repo}`;

    if (request.method === 'GET' && action === 'info/refs' && service === 'git-upload-pack') {
      // Git clone
      const head = await env.R2_BUCKET.get(`${repoPrefix}/HEAD`);
      let body;
      if (head === null) {
        // No HEAD, repository is empty
        body = [
          pktLine('# service=git-upload-pack'),
          flushPkt(),
          pktLine('0000000000000000000000000000000000000000 HEAD\0multi_ack thin-pack side-band side-band-64k ofs-delta shallow deepen-since deepen-not deepen-relative no-progress include-tag multi_ack_detailed symref=HEAD:refs/heads/master'),
          flushPkt(),
        ].join('');
      } else {
        const headRefPath = (await head.text()).trim().substring(5); // 'ref: refs/heads/master' -> 'refs/heads/master'
        const headRef = await env.R2_BUCKET.get(`${repoPrefix}/${headRefPath}`);
        if (headRef === null) {
          // Should not happen in a consistent repository
          return new Response('Not found', { status: 404 });
        }
        const headSha = (await headRef.text()).trim();
        body = [
          pktLine('# service=git-upload-pack'),
          flushPkt(),
          pktLine(`${headSha} HEAD\0multi_ack thin-pack side-band side-band-64k ofs-delta shallow deepen-since deepen-not deepen-relative no-progress include-tag multi_ack_detailed symref=HEAD:${headRefPath}`),
          pktLine(`${headSha} ${headRefPath}`),
          flushPkt(),
        ].join('');
      }


      return new Response(body, {
        headers: {
          'Content-Type': 'application/x-git-upload-pack-advertisement',
          'Cache-Control': 'no-cache',
        },
      });
    } else if (request.method === 'POST' && action === 'git-receive-pack' && service === 'git-receive-pack') {
      // Git push
      if (request.body === null) {
        return new Response('No body', { status: 400 });
      }

      const reader = request.body.getReader();

      let line = await parsePktLine(reader);
      while (line !== null) {
        const [oldSha, newSha, ref] = line.trim().split(' ');
        if (ref) {
          await env.R2_BUCKET.put(`${repoPrefix}/${ref}`, newSha);
          const head = await env.R2_BUCKET.get(`${repoPrefix}/HEAD`);
          if (head === null) {
            await env.R2_BUCKET.put(`${repoPrefix}/HEAD`, `ref: ${ref}`);
          }
        }
        line = await parsePktLine(reader);
      }

      // TODO: Process the packfile
      return new Response(null, { status: 200 });
    }

    return new Response('Not found', { status: 404 });
  },
};
