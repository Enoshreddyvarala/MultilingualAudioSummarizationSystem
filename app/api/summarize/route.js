/**
 * MLYTAS Pipeline Orchestrator — Vercel API Route
 * Receives an audio file, calls HF Space 1 (ASR) then HF Space 2 (Summarizer)
 * using the Gradio 5 Queue/SSE protocol, and streams results back to the client.
 */

import { Client } from '@gradio/client/dist/index.js';

export const maxDuration = 300; // 5 minutes max duration for Vercel
export const dynamic = 'force-dynamic';

const ASR_URL = process.env.HF_SPACE_ASR_URL;
const SUMMARIZER_URL = process.env.HF_SPACE_SUMMARIZER_URL;
const HF_TOKEN = process.env.HF_TOKEN;

function sseEvent(type, data) {
  return `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
}

async function callGradioQueue(baseUrl, apiName, dataArray, timeoutMs = 300_000) {
  const client = await Client.connect(baseUrl, {
    token: HF_TOKEN,
    events: ['data', 'status']
  });

  const submission = client.submit(`/${apiName}`, dataArray);
  let lastData = null;
  let lastError = null;
  let done = false;

  const timeout = setTimeout(async () => {
    if (submission.cancel) {
      await submission.cancel();
    }
  }, timeoutMs);

  try {
    for await (const msg of submission) {
      if (msg.type === 'data') {
        lastData = msg.data;
      }
      if (msg.type === 'status' && msg.stage === 'error') {
        lastError = Array.isArray(msg.message)
          ? msg.message.map((item) => item?.message ?? item).join(' ')
          : msg.message || 'Gradio reported an error.';
      }
      if (msg.type === 'status' && msg.stage === 'complete') {
        done = true;
      }
    }
  } finally {
    clearTimeout(timeout);
  }

  if (lastError) {
    throw new Error(lastError);
  }
  if (!done) {
    throw new Error('Gradio request did not complete successfully.');
  }
  if (lastData === null) {
    throw new Error('No data returned from Gradio endpoint.');
  }

  return lastData;
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
        // ── Step 1: Prepare ASR request ────────────────────────────────────
        send('stage', { stage: 'uploading' });

        // ── Step 2: ASR ──────────────────────────────────────────────────
        send('stage', { stage: 'transcribing' });

        let transcript;
        try {
          const raw = await callGradioQueue(ASR_URL, 'transcribe', [file], 1_200_000);
          
          if (!raw || !Array.isArray(raw)) {
            throw new Error('Invalid response structure from ASR service.');
          }

          const transcriptText = raw[0];
          const metadata = raw[1] || {};

          if (!transcriptText) {
            throw new Error('No transcription returned from ASR service.');
          }

          transcript = {
            text: transcriptText,
            language: metadata.language || '',
            duration: metadata.duration || 0,
            segments: metadata.segments_list || []
          };
          // Emit the raw transcript to the client so the UI can show it
          try {
            send('section', { name: 'transcript', content: transcript });
          } catch (err) {
            // ignore send errors here; continue to summarization
          }
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
          const transcriptStr = JSON.stringify(transcript);
          const raw = await callGradioQueue(SUMMARIZER_URL, 'summarize', [transcriptStr, lang], 180_000);
          
          let analysisRaw = raw?.[0];
          if (typeof analysisRaw === 'string') {
            analysis = JSON.parse(analysisRaw);
          } else {
            analysis = analysisRaw;
          }

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
