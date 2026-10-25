// functions/api/crawl.js
const AUDIO_EXT = /\.(mp3|mp2|wav|ogg|oga|m4a|m4b|flac|aac|opus|weba|wma|aiff|aif|aifc|ape|mka|mpc|m3u|m3u8|pls|xspf)$/i;
const CONCURRENCY = 6;

export async function onRequestGet(context) {
  return handleCrawl(context, null);
}

export async function onRequestPost(context) {
  let resume = null;
  try {
    const body = await context.request.json();
    if (body && body.resume && typeof body.resume === 'object') resume = body.resume;
  } catch (_) {}
  return handleCrawl(context, resume);
}

async function handleCrawl(context, resume) {
  const { request } = context;
  const q = new URL(request.url).searchParams;

  const startUrl = (q.get('url') || '').trim();
  const scopeParam = (q.get('scope') || '').trim().toLowerCase();
  let maxPages = parseInt(q.get('maxPages') || '100', 10);
  let maxDepth = parseInt(q.get('maxDepth') || '3', 10);
  if (!Number.isFinite(maxPages)) maxPages = 100;
  if (!Number.isFinite(maxDepth)) maxDepth = 3;
  maxPages = Math.max(1, Math.min(maxPages, 2000));
  maxDepth = Math.max(0, Math.min(maxDepth, 10));

  if (!startUrl) return json({ error: 'url required' }, 400);

  let start;
  try { start = new URL(startUrl); } catch { return json({ error: 'invalid url' }, 400); }
  if (start.protocol !== 'http:' && start.protocol !== 'https:') {
    return json({ error: 'only http/https allowed' }, 400);
  }

  const scopeDomain = (scopeParam || start.hostname).toLowerCase().replace(/^www\./, '');
  const inScope = (hostname) => {
    hostname = hostname.toLowerCase();
    return hostname === scopeDomain || hostname.endsWith('.' + scopeDomain) || hostname === 'www.' + scopeDomain;
  };

  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    async start(controller) {
      let closed = false;
      const send = (obj) => {
        if (closed) return;
        try { controller.enqueue(encoder.encode(JSON.stringify(obj) + '\n')); }
        catch { closed = true; }
      };

      const normalize = (u) => {
        try { const n = new URL(u); n.hash = ''; return n.href; }
        catch { return u; }
      };

      // ---- resume-aware initialization ----
      const visited = new Set(Array.isArray(resume?.visited) ? resume.visited : []);
      const seenFiles = new Set(Array.isArray(resume?.seenFiles) ? resume.seenFiles : []);
      const queue = Array.isArray(resume?.queue) && resume.queue.length
        ? resume.queue.slice()
        : [{ url: start.href, depth: 0, source: null }];

      if (!resume) visited.add(normalize(start.href));

      let pagesAttempted = resume?.stats?.pages || 0;   // cumulative
      let fileCount     = resume?.stats?.files || 0;    // cumulative
      let failed        = resume?.stats?.failed || 0;   // cumulative
      let pagesThisWorker = 0;                          // per-worker budget

      try {
        while (queue.length > 0 && pagesThisWorker < maxPages) {
          const batch = [];
          while (queue.length > 0 && batch.length < CONCURRENCY && pagesThisWorker + batch.length < maxPages) {
            const item = queue.shift();
            try { if (!inScope(new URL(item.url).hostname)) continue; }
            catch { continue; }
            batch.push(item);
          }
          if (batch.length === 0) break;

          const results = await Promise.allSettled(batch.map(it => fetchPage(it.url)));

          for (let i = 0; i < batch.length; i++) {
            const item = batch[i];
            const res = results[i];
            pagesAttempted++;
            pagesThisWorker++;

            if (res.status !== 'fulfilled') {
              failed++;
              const reason = res.reason && res.reason.message ? res.reason.message : 'fetch failed';
              send({ type: 'page', url: item.url, source: item.source, depth: item.depth, status: 'fail', reason: reason });
              continue;
            }

            let links;
            try {
              links = await extractLinks(res.value, item.url);
            } catch (e) {
              failed++;
              send({ type: 'page', url: item.url, source: item.source, depth: item.depth, status: 'fail', reason: 'parse error' });
              continue;
            }

            send({ type: 'page', url: item.url, source: item.source, depth: item.depth, status: 'ok', links: links.length });

            for (const abs of links) {
              if (AUDIO_EXT.test(abs.pathname)) {
                if (!seenFiles.has(abs.href)) {
                  seenFiles.add(abs.href);
                  fileCount++;
                  send({
                    type: 'file',
                    file: {
                      host: abs.hostname,
                      name: safeDecode(abs.pathname.split('/').pop() || 'file'),
                      url: abs.href,
                      page: item.url
                    }
                  });
                }
                continue;
              }

              if (item.depth < maxDepth && inScope(abs.hostname)) {
                const key = normalize(abs.href);
                if (visited.has(key)) continue;
                visited.add(key);
                queue.push({ url: abs.href, depth: item.depth + 1, source: item.url });
              }
            }
          }
        }

        const resumeNext = queue.length > 0 ? {
          visited: Array.from(visited),
          seenFiles: Array.from(seenFiles),
          queue,
          stats: { pages: pagesAttempted, files: fileCount, failed }
        } : null;

        send({
          type: 'done',
          stats: { pages: pagesAttempted, files: fileCount, failed },
          resume: resumeNext
        });
      } catch (err) {
        send({ type: 'error', message: String(err && err.message ? err.message : err) });
        send({
          type: 'done',
          stats: { pages: pagesAttempted, files: fileCount, failed },
          resume: queue.length > 0 ? {
            visited: Array.from(visited),
            seenFiles: Array.from(seenFiles),
            queue,
            stats: { pages: pagesAttempted, files: fileCount, failed }
          } : null
        });
      } finally {
        if (!closed) { try { controller.close(); } catch (_) {} }
      }
    },
    cancel() { }
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'application/x-ndjson; charset=utf-8',
      'Cache-Control': 'no-store',
      'X-Accel-Buffering': 'no'
    }
  });
}

async function fetchPage(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; AudioCrawler/1.0; +https://pages.dev)',
      'Accept': 'text/html,application/xhtml+xml'
    },
    redirect: 'follow',
    cf: { cacheTtl: 120, cacheEverything: false }
  });
  if (!res.ok) throw new Error('HTTP ' + res.status);
  const ct = (res.headers.get('content-type') || '').toLowerCase();
  if (!ct.includes('text/html') && !ct.includes('application/xhtml')) {
    throw new Error('not html');
  }
  return res;
}

async function extractLinks(response, baseUrl) {
  const base = new URL(baseUrl);
  const hrefs = new Set();
  const addHref = (h) => {
    if (!h) return;
    h = h.trim();
    if (!h) return;
    if (h.startsWith('#') || h.startsWith('javascript:') || h.startsWith('mailto:') ||
        h.startsWith('tel:') || h.startsWith('data:') || h.startsWith('blob:')) return;
    hrefs.add(h);
  };

  const rewriter = new HTMLRewriter()
    .on('a[href]',            { element(e) { addHref(e.getAttribute('href')); } })
    .on('area[href]',         { element(e) { addHref(e.getAttribute('href')); } })
    .on('link[href]',         { element(e) { addHref(e.getAttribute('href')); } })
    .on('audio[src]',         { element(e) { addHref(e.getAttribute('src')); } })
    .on('audio source[src]',  { element(e) { addHref(e.getAttribute('src')); } })
    .on('video[src]',         { element(e) { addHref(e.getAttribute('src')); } })
    .on('video source[src]',  { element(e) { addHref(e.getAttribute('src')); } })
    .on('source[src]',        { element(e) { addHref(e.getAttribute('src')); } })
    .on('meta[property="og:audio"]',          { element(e) { addHref(e.getAttribute('content')); } })
    .on('meta[property="og:audio:url"]',      { element(e) { addHref(e.getAttribute('content')); } })
    .on('meta[name="twitter:player:stream"]', { element(e) { addHref(e.getAttribute('content')); } });

  await rewriter.transform(response).text();

  const out = [];
  for (const href of hrefs) {
    try {
      const abs = new URL(href, base);
      if (abs.protocol !== 'http:' && abs.protocol !== 'https:') continue;
      abs.hash = '';
      out.push(abs);
    } catch (_) {}
  }
  return out;
}

function safeDecode(s) { try { return decodeURIComponent(s); } catch { return s; } }
function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json' }
  });
}
