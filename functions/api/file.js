// functions/api/file.js
export async function onRequest(context) {
  const { request } = context;
  const url = new URL(request.url);
  const fileUrl = url.searchParams.get('url');
  const isDownload = url.searchParams.get('dl') === '1';

  if (!fileUrl) {
    return new Response('Missing url parameter', { status: 400 });
  }

  try {
    // 1. Prepare headers for the origin server
    const originHeaders = new Headers();
    // Pass the Range header to support audio seeking (skipping forward/back)
    const range = request.headers.get('Range');
    if (range) {
      originHeaders.set('Range', range);
    }
    // Some servers block requests without a User-Agent
    originHeaders.set('User-Agent', 'Mozilla/5.0 (compatible; AudioCrawler/1.0)');

    // 2. Fetch the file from the remote server
    const response = await fetch(fileUrl, { headers: originHeaders });

    if (!response.ok && response.status !== 206) {
      return new Response('Failed to fetch file: ' + response.status, { status: response.status });
    }

    // 3. Prepare the response headers
    const newHeaders = new Headers(response.headers);
    
    // If it's a download, force the browser to download it as an attachment
    if (isDownload) {
      // Extract filename from URL, default to 'audio'
      let filename = 'audio';
      try {
        const parts = new URL(fileUrl).pathname.split('/');
        if (parts.length > 0) filename = decodeURIComponent(parts.pop() || 'audio');
      } catch (_) {}
      newHeaders.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      // If it's for playing, we need to ensure CORS is open so the browser accepts it
      newHeaders.set('Access-Control-Allow-Origin', '*');
      newHeaders.set('Accept-Ranges', 'bytes');
    }

    // 4. Return the streamed response
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: newHeaders
    });

  } catch (err) {
    return new Response('Error proxying file: ' + err.message, { status: 500 });
  }
}
