/**
 * MLYTAS Pipeline Orchestrator — Vercel API Route
 * Receives a YouTube URL, calls HF Space 1 (ASR) then HF Space 2 (Summarizer),
 * and streams results back to the client as Server-Sent Events.
 */

const ASR_URL = process.env.HF_SPACE_ASR_URL;
const SUMMARIZER_URL = process.env.HF_SPACE_SUMMARIZER_URL;

function sseEvent(type, data) {
  return `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
}

/**
 * Call a Gradio Space's /api/predict endpoint.
 */
async function callGradio(baseUrl, inputs, apiName, timeoutMs = 300_000) {
  const endpoint = `${baseUrl}/api/predict`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // api_name handles correct routing in newer Gradio versions
      body: JSON.stringify({ data: inputs, api_name: `/${apiName}` }),
      signal: controller.signal,
    });

    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`HF Space responded ${res.status}: ${text.slice(0, 200)}`);
    }

    const json = await res.json();
    return json.data?.[0] ?? json.data;
  } finally {
    clearTimeout(timer);
  }
}

export async function POST(request) {
  let formData;
  try {
    formData = await request.formData();
  } catch (err) {
    return new Response(
      sseEvent('error', { message: 'Failed to parse form data.' }) + sseEvent('done', {}),
      { status: 400, headers: { 'Content-Type': 'text/event-stream' } }
    );
  }

  const file = formData.get('audio');
  const lang = formData.get('lang') ?? 'english';

  if (!file || typeof file === 'string') {
    return new Response(
      sseEvent('error', { message: 'No valid audio file provided.' }) + sseEvent('done', {}),
      { status: 400, headers: { 'Content-Type': 'text/event-stream' } }
    );
  }

  if (!ASR_URL || !SUMMARIZER_URL) {
    return new Response(
      sseEvent('error', { message: 'Backend services are not configured.' }) + sseEvent('done', {}),
      { status: 500, headers: { 'Content-Type': 'text/event-stream' } }
    );
  }

  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    async start(controller) {
      function send(type, data) {
        controller.enqueue(encoder.encode(sseEvent(type, data)));
      }

      try {
        // ── Step 1: Upload to Gradio ──────────────────────────────────────
        send('stage', { stage: 'uploading' });

        const uploadData = new FormData();
        uploadData.append('files', file);

        const uploadRes = await fetch(`${ASR_URL}/upload`, {
          method: 'POST',
          body: uploadData,
        });

        if (!uploadRes.ok) {
          throw new Error(`File upload failed with status ${uploadRes.status}`);
        }
        
        const uploadJson = await uploadRes.json();
        // Gradio 4/5 returns an array of file objects
        const uploadedFileMeta = uploadJson[0]; 
        
        // Prepare the FileData dictionary for the predict endpoint
        const fileInput = {
          path: uploadedFileMeta.path || uploadedFileMeta.name,
          orig_name: uploadedFileMeta.orig_name,
          meta: { _type: "gradio.FileData" }
        };

        // ── Step 2: ASR ──────────────────────────────────────────────────
        send('stage', { stage: 'transcribing' });

        let transcript;
        try {
          const raw = await callGradio(ASR_URL, [fileInput], 'transcribe', 1_200_000);
          transcript = typeof raw === 'string' ? JSON.parse(raw) : raw;
          if (transcript?.error) throw new Error(transcript.error);
        } catch (err) {
          send('error', { message: `Transcription failed: ${err.message}` });
          send('done', {});
          controller.close();
          return;
        }

        send('stage', { stage: 'summarizing' });

        // ── Step 3: Summarization ────────────────────────────────────────
        let analysis;
        try {
          const transcriptStr = typeof transcript === 'string' ? transcript : JSON.stringify(transcript);
          const raw = await callGradio(SUMMARIZER_URL, [transcriptStr, lang], 'summarize', 180_000);
          analysis = typeof raw === 'string' ? JSON.parse(raw) : raw;
          if (analysis?.error) throw new Error(analysis.error);
        } catch (err) {
          send('error', { message: `Summarization failed: ${err.message}` });
          send('done', {});
          controller.close();
          return;
        }

        // ── Stream sections one-by-one ───────────────────────────────────
        const SECTION_KEYS = [
          'concise_summary', 'detailed_summary', 'topics', 'speaker_insights',
          'action_items', 'key_quotes', 'translation',
        ];

        send('section', {
          name: '__meta__',
          content: {
            title: analysis.video_title ?? transcript?.video_title ?? file.name,
            duration_formatted: analysis.duration_formatted ?? '',
            language_detected: analysis.language_detected ?? transcript?.language ?? '',
          },
        });

        for (const key of SECTION_KEYS) {
          if (analysis[key] !== undefined) {
            send('section', { name: key, content: analysis[key] });
            await new Promise((r) => setTimeout(r, 120));
          }
        }

        send('done', {});
      } catch (err) {
        try {
          controller.enqueue(encoder.encode(sseEvent('error', { message: err.message ?? 'Unknown error' })));
          controller.enqueue(encoder.encode(sseEvent('done', {})));
        } catch {}
      } finally {
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  });
}
