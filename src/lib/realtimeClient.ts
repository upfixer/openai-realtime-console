import OpenAI from 'openai';
import { OpenAIRealtimeWebSocket } from 'openai/realtime/websocket';

type RealtimeSocket = OpenAIRealtimeWebSocket;

export type ItemType = {
  id: string;
  type: string;
  role?: 'user' | 'assistant' | 'system';
  status?: 'in_progress' | 'completed' | 'incomplete';
  content?: Array<any>;
  call_id?: string;
  name?: string;
  arguments?: string;
  output?: string;
  formatted: {
    audio?: Int16Array;
    text?: string;
    transcript?: string;
    tool?: { type: 'function'; name: string; call_id: string; arguments: string };
    output?: string;
    file?: any;
  };
};

class RealtimeConversation {
  defaultFrequency = 24_000;
  itemLookup: Record<string, ItemType> = {};
  items: ItemType[] = [];
  responseLookup: Record<string, any> = {};
  responses: any[] = [];
  queuedSpeechItems: Record<string, any> = {};
  queuedTranscriptItems: Record<string, any> = {};
  queuedInputAudio: Int16Array | null = null;


  clear() {
    this.itemLookup = {};
    this.items = [];
    this.responseLookup = {};
    this.responses = [];
    this.queuedSpeechItems = {};
    this.queuedTranscriptItems = {};
    this.queuedInputAudio = null;
  }

  queueInputAudio(inputAudio: Int16Array) {
    this.queuedInputAudio = inputAudio;
  }

  getItem(id: string) {
    return this.itemLookup[id] || null;
  }

  getItems() {
    return this.items.slice();
  }

  processEvent(event: any, inputAudioBuffer?: Int16Array) {
    const type = event.type;
    if (type === 'conversation.item.created' || type === 'conversation.item.added' || type === 'conversation.item.done') {
      const newItem: ItemType = JSON.parse(JSON.stringify(event.item));
      if (!this.itemLookup[newItem.id]) {
        this.itemLookup[newItem.id] = newItem;
        this.items.push(newItem);
      }
      newItem.formatted = { audio: new Int16Array(0), text: '', transcript: '' };
      if (this.queuedSpeechItems[newItem.id]?.audio) {
        newItem.formatted.audio = this.queuedSpeechItems[newItem.id].audio;
        delete this.queuedSpeechItems[newItem.id];
      }
      if (newItem.content) {
        const textContent = newItem.content.filter((c: any) =>
          ['text', 'input_text', 'output_text'].includes(c.type)
        );
        for (const content of textContent) {
          newItem.formatted.text = `${newItem.formatted.text || ''}${content.text || ''}`;
        }
      }
      if (this.queuedTranscriptItems[newItem.id]) {
        newItem.formatted.transcript = this.queuedTranscriptItems[newItem.id].transcript;
        delete this.queuedTranscriptItems[newItem.id];
      }
      if (newItem.type === 'message') {
        if (newItem.role === 'user') {
          newItem.status = 'completed';
          if (this.queuedInputAudio) {
            newItem.formatted.audio = this.queuedInputAudio;
            this.queuedInputAudio = null;
          }
        } else {
          newItem.status = 'in_progress';
        }
      } else if (newItem.type === 'function_call') {
        newItem.formatted.tool = {
          type: 'function',
          name: newItem.name || '',
          call_id: newItem.call_id || '',
          arguments: '',
        };
        newItem.status = 'in_progress';
      } else if (newItem.type === 'function_call_output') {
        newItem.status = 'completed';
        newItem.formatted.output = newItem.output;
      }
      return { item: newItem, delta: null };
    }

    if (type === 'conversation.item.truncated') {
      const item = this.itemLookup[event.item_id];
      if (!item) return { item: null, delta: null };
      const endIndex = Math.floor((event.audio_end_ms * this.defaultFrequency) / 1000);
      item.formatted.transcript = '';
      item.formatted.audio = (item.formatted.audio || new Int16Array(0)).slice(0, endIndex);
      return { item, delta: null };
    }

    if (type === 'conversation.item.deleted') {
      const item = this.itemLookup[event.item_id];
      if (!item) return { item: null, delta: null };
      delete this.itemLookup[item.id];
      const idx = this.items.indexOf(item);
      if (idx > -1) this.items.splice(idx, 1);
      return { item, delta: null };
    }

    if (type === 'conversation.item.input_audio_transcription.completed') {
      const item = this.itemLookup[event.item_id];
      const formattedTranscript = event.transcript || ' ';
      if (!item) {
        this.queuedTranscriptItems[event.item_id] = { transcript: formattedTranscript };
        return { item: null, delta: null };
      }
      item.content = item.content || [];
      if (item.content[event.content_index]) {
        item.content[event.content_index].transcript = event.transcript;
      }
      item.formatted.transcript = formattedTranscript;
      return { item, delta: { transcript: event.transcript } };
    }

    if (type === 'input_audio_buffer.speech_started') {
      this.queuedSpeechItems[event.item_id] = { audio_start_ms: event.audio_start_ms };
      return { item: null, delta: null };
    }

    if (type === 'input_audio_buffer.speech_stopped') {
      const speech = this.queuedSpeechItems[event.item_id] || {};
      speech.audio_end_ms = event.audio_end_ms;
      if (inputAudioBuffer?.byteLength) {
        const startIndex = Math.floor((speech.audio_start_ms * this.defaultFrequency) / 1000);
        const endIndex = Math.floor((speech.audio_end_ms * this.defaultFrequency) / 1000);
        speech.audio = inputAudioBuffer.slice(startIndex, endIndex);
      }
      this.queuedSpeechItems[event.item_id] = speech;
      return { item: null, delta: null };
    }

    if (type === 'response.created') {
      const response = event.response;
      if (!this.responseLookup[response.id]) {
        this.responseLookup[response.id] = response;
        this.responses.push(response);
      }
      return { item: null, delta: null };
    }

    if (type === 'response.output_item.added') {
      const response = this.responseLookup[event.response_id];
      if (response) {
        response.output = response.output || [];
        response.output.push(event.item.id);
      }
      return { item: null, delta: null };
    }

    if (type === 'response.output_item.done') {
      const item = this.itemLookup[event.item.id];
      if (!item) return { item: null, delta: null };
      item.status = event.item.status;
      return { item, delta: null };
    }

    if (type === 'response.content_part.added') {
      const item = this.itemLookup[event.item_id];
      if (!item) return { item: null, delta: null };
      item.content = item.content || [];
      item.content.push(event.part);
      return { item, delta: null };
    }

    if (type === 'response.output_audio_transcript.delta') {
      const item = this.itemLookup[event.item_id];
      if (!item) return { item: null, delta: null };
      item.content = item.content || [];
      item.content[event.content_index] = item.content[event.content_index] || {};
      item.content[event.content_index].transcript = `${item.content[event.content_index].transcript || ''}${event.delta}`;
      item.formatted.transcript = `${item.formatted.transcript || ''}${event.delta}`;
      return { item, delta: { transcript: event.delta } };
    }

    if (type === 'response.output_audio.delta') {
      const item = this.itemLookup[event.item_id];
      if (!item) return { item: null, delta: null };
      const appendValues = base64ToInt16(event.delta);
      item.formatted.audio = mergeInt16(item.formatted.audio || new Int16Array(0), appendValues);
      return { item, delta: { audio: appendValues } };
    }

    if (type === 'response.output_text.delta') {
      const item = this.itemLookup[event.item_id];
      if (!item) return { item: null, delta: null };
      item.content = item.content || [];
      item.content[event.content_index] = item.content[event.content_index] || {};
      item.content[event.content_index].text = `${item.content[event.content_index].text || ''}${event.delta}`;
      item.formatted.text = `${item.formatted.text || ''}${event.delta}`;
      return { item, delta: { text: event.delta } };
    }

    if (type === 'response.function_call_arguments.delta') {
      const item = this.itemLookup[event.item_id];
      if (!item) return { item: null, delta: null };
      item.arguments = `${item.arguments || ''}${event.delta}`;
      if (item.formatted.tool) {
        item.formatted.tool.arguments = `${item.formatted.tool.arguments || ''}${event.delta}`;
      }
      return { item, delta: { arguments: event.delta } };
    }

    return { item: null, delta: null };
  }
}

function int16ToBase64(arrayBuffer: Int16Array | ArrayBuffer) {
  const view = arrayBuffer instanceof Int16Array ? new Uint8Array(arrayBuffer.buffer) : new Uint8Array(arrayBuffer);
  let binary = '';
  const chunk = 0x8000;
  for (let i = 0; i < view.length; i += chunk) {
    binary += String.fromCharCode(...view.subarray(i, i + chunk));
  }
  return btoa(binary);
}

function base64ToInt16(base64: string) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return new Int16Array(bytes.buffer);
}

function mergeInt16(left: Int16Array, right: Int16Array | ArrayBuffer) {
  const rightView = right instanceof Int16Array ? right : new Int16Array(right);
  const out = new Int16Array(left.length + rightView.length);
  out.set(left, 0);
  out.set(rightView, left.length);
  return out;
}

class Emitter {
  private handlers: Record<string, Set<(data: any) => void>> = {};
  on(event: string, cb: (data: any) => void) {
    this.handlers[event] = this.handlers[event] || new Set();
    this.handlers[event].add(cb);
  }
  off(event: string, cb: (data: any) => void) {
    this.handlers[event]?.delete(cb);
  }
  dispatch(event: string, data?: any) {
    this.handlers[event]?.forEach((cb) => cb(data));
  }
  clear() {
    this.handlers = {};
  }
}

export class RealtimeClient extends Emitter {
  sessionCreated = false;
  sessionConfig: any;
  defaultSessionConfig: any;
  tools: Record<string, { definition: any; handler: (args: any) => Promise<any> | any }> = {};
  realtime: RealtimeSocket | null = null;
  disconnectingRealtime: RealtimeSocket | null = null;
  conversation = new RealtimeConversation();
  inputAudioBuffer = new Int16Array(0);
  apiKey: string;
  dangerouslyAllowAPIKeyInBrowser: boolean;
  model: string;
  baseUrl: string;
  isGreetingSent: Boolean = false;

  constructor({ apiKey, dangerouslyAllowAPIKeyInBrowser = true, model = 'gpt-realtime', baseUrl = 'https://api.openai.com/v1' }: any = {}) {
    super();
    this.apiKey = apiKey;
    this.dangerouslyAllowAPIKeyInBrowser = dangerouslyAllowAPIKeyInBrowser;
    this.model = model;
    this.baseUrl = baseUrl;
    this.defaultSessionConfig = {
      type: "realtime",
      output_modalities: ['audio'],
      instructions: 'Speak clearly and briefly. Confirm understanding before taking actions.',
      audio: {
        input: {
          format: {
            type: "audio/pcm",
            rate: 24000,
          },
          turn_detection: {
            type: "server_vad",
          },
        },
        output: {
          format: {
            type: "audio/pcm",
            rate: 24000
          },
          voice: "marin",
        }
      },
    };
    this.sessionConfig = { ...this.defaultSessionConfig };
  }

  isConnected() {
    return !!this.realtime;
  }

  getTurnDetectionType() {
    return this.sessionConfig.turn_detection?.type || null;
  }

  async connect() {
    if (this.realtime) throw new Error('Already connected, use .disconnect() first');
    const client = new OpenAI({
      baseURL: this.baseUrl,
      apiKey: this.apiKey,
      dangerouslyAllowBrowser: true,
    });
    const onURL = (url: URL) => {
      if (url.hostname == "127.0.0.1") {
        url.protocol="ws"
      }
      console.log(url)
    }
    const rt = new OpenAIRealtimeWebSocket({ model: this.model, dangerouslyAllowBrowser: true, onURL: onURL }, client);
    this.realtime = rt;

    rt.on('event', (event: any) => {
      this.dispatch('realtime.event', {
        time: new Date().toISOString(),
        source: 'server',
        event,
      });
    });
    rt.on('error', (error: any) => this.dispatch('error', error));
    rt.socket?.addEventListener?.('close', (event: CloseEvent) => {
      const wasIntentionalClose = this.disconnectingRealtime === rt;
      if (wasIntentionalClose) this.disconnectingRealtime = null;
      if (this.realtime === rt) this.realtime = null;

      this.sessionCreated = false;
      this.isGreetingSent = false;

      const closedEvent = {
        type: 'closed',
        event_id: `connection.closed.${Date.now()}`,
        code: event.code,
        reason: event.reason,
        was_clean: event.wasClean,
      };
      this.dispatch('realtime.event', {
        time: new Date().toISOString(),
        source: 'server',
        event: closedEvent,
      });
      if (!wasIntentionalClose) {
        this.dispatch('connection.closed', closedEvent);
      }
    });

    rt.on('session.created', () => {
      console.log("!!!session.created")
      this.sessionCreated = true;
      this.updateSession();
    });


    const handler = (event: any, ...args: any[]) => this.conversation.processEvent(event, ...args);
    const handlerWithDispatch = (event: any, ...args: any[]) => {
      const { item, delta } = handler(event, ...args);
      if (item) this.dispatch('conversation.updated', { item, delta });
      return { item, delta };
    };

    const callTool = async (tool: any) => {
      try {
        const jsonArguments = JSON.parse(tool.arguments || '{}');
        const toolConfig = this.tools[tool.name];
        if (!toolConfig) throw new Error(`Tool "${tool.name}" has not been added`);
        const result = await toolConfig.handler(jsonArguments);
        this.send('conversation.item.create', {
          item: { type: 'function_call_output', call_id: tool.call_id, output: JSON.stringify(result) },
        });
      } catch (e: any) {
        this.send('conversation.item.create', {
          item: { type: 'function_call_output', call_id: tool.call_id, output: JSON.stringify({ error: e.message }) },
        });
      }
      this.createResponse();
    };

    rt.on('session.updated', (event: any) => {
      handler(event);
      if (!this.isGreetingSent) {
        this.isGreetingSent = true;
        this.sendUserMessageContent([
          {
            type: `input_text`,
            text: `Hello!`,
            // text: `For testing purposes, I want you to list ten car brands. Number each item, e.g. "one (or whatever number you are one): the item name".`
          },
        ]);
        console.log("Greeting sent")
      }
    });
    rt.on('response.created', handler);
    rt.on('response.output_item.added', handler);
    rt.on('response.content_part.added', handler);
    rt.on('input_audio_buffer.speech_started', (event: any) => {
      handler(event);
      this.dispatch('conversation.interrupted');
    });
    rt.on('input_audio_buffer.speech_stopped', (event: any) => handler(event, this.inputAudioBuffer));

    rt.on('conversation.item.created', (event: any) => {
      const { item } = handlerWithDispatch(event);
      if (!item) return;
      this.dispatch('conversation.item.appended', { item });
      if (item.status === 'completed') this.dispatch('conversation.item.completed', { item });
    });
    rt.on('conversation.item.added', (event: any) => {
      const { item } = handlerWithDispatch(event);
      if (!item) return;
      this.dispatch('conversation.item.appended', { item });
      if (item.status === 'completed') this.dispatch('conversation.item.completed', { item });
    });
    rt.on('conversation.item.truncated', handlerWithDispatch);
    rt.on('conversation.item.deleted', handlerWithDispatch);
    rt.on('conversation.item.input_audio_transcription.completed', handlerWithDispatch);
    rt.on('response.output_audio_transcript.delta', handlerWithDispatch);
    rt.on('response.output_audio.delta', handlerWithDispatch);
    rt.on('response.output_text.delta', handlerWithDispatch);
    rt.on('response.function_call_arguments.delta', handlerWithDispatch);
    rt.on('response.output_item.done', async (event: any) => {
      const { item } = handlerWithDispatch(event);
      if (!item) return;
      if (item.status === 'completed') this.dispatch('conversation.item.completed', { item });
      if (item.formatted.tool) await callTool(item.formatted.tool);
    });

    await new Promise<void>((resolve) => {
      if (this.sessionCreated) {
        resolve();
        return;
      }
      const done = () => {
        if (this.sessionCreated) {
          rt.off('session.created', done);
          resolve();
        }
      };
      rt.on('session.created', done);
    });
  }

  disconnect() {
    this.sessionCreated = false;
    this.conversation.clear();
    const realtime = this.realtime;
    if (realtime) {
      this.disconnectingRealtime = realtime;
      realtime.close();
    }
    this.realtime = null;
    this.isGreetingSent = false;
  }

  reset() {
    this.disconnect();
    this.clear();
    this.tools = {};
    this.sessionConfig = { ...this.defaultSessionConfig };
    this.inputAudioBuffer = new Int16Array(0);
  }

  private send(type: string, payload: Record<string, any> = {}) {
    const event = { type, ...payload };
    this.dispatch('realtime.event', {
      time: new Date().toISOString(),
      source: 'client',
      event,
    });
    this.realtime?.send(event as any);
  }

  addTool(definition: any, handler: any) {
    if (!definition?.name) throw new Error('Missing tool name in definition');
    this.tools[definition.name] = { definition, handler };
    this.updateSession();
    return this.tools[definition.name];
  }

  deleteItem(id: string) {
    this.send('conversation.item.delete', { item_id: id });
  }

  updateSession(sessionConfig: any = {}) {
    Object.assign(this.sessionConfig, sessionConfig);
    const useTools = Object.keys(this.tools).map((key) => ({
      type: 'function',
      ...this.tools[key].definition,
    }));
    const session = { ...this.sessionConfig, tools: useTools };
    if (this.realtime) this.send('session.update', { session });
  }

  sendUserMessageContent(content: Array<any> = []) {
    if (content.length) {
      for (const c of content) {
        if (c.type === 'input_audio' && (c.audio instanceof ArrayBuffer || c.audio instanceof Int16Array)) {
          c.audio = int16ToBase64(c.audio);
        }
      }
      this.send('conversation.item.create', {
        item: { type: 'message', role: 'user', content },
      });
    }
    this.createResponse();
  }

  appendInputAudio(arrayBuffer: Int16Array | ArrayBuffer) {
    if (arrayBuffer.byteLength > 0) {
      this.send('input_audio_buffer.append', { audio: int16ToBase64(arrayBuffer) });
      this.inputAudioBuffer = mergeInt16(this.inputAudioBuffer, arrayBuffer);
    }
  }

  createResponse() {
    if (this.getTurnDetectionType() === null && this.inputAudioBuffer.byteLength > 0) {
      this.send('input_audio_buffer.commit');
      this.conversation.queueInputAudio(this.inputAudioBuffer);
      this.inputAudioBuffer = new Int16Array(0);
    }
    this.send('response.create');
  }

  async cancelResponse(id?: string, sampleCount = 0) {
    if (!id) {
      this.send('response.cancel');
      return { item: null };
    }
    const item = this.conversation.getItem(id);
    if (!item) throw new Error(`Could not find item "${id}"`);
    this.send('response.cancel');
    const audioIndex = (item.content || []).findIndex((c: any) => ['output_audio', 'audio'].includes(c.type));
    if (audioIndex > -1) {
      this.send('conversation.item.truncate', {
        item_id: id,
        content_index: audioIndex,
        audio_end_ms: Math.floor((sampleCount / this.conversation.defaultFrequency) * 1000),
      });
    }
    return { item };
  }
}
