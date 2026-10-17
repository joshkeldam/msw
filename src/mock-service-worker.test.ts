import fs from 'node:fs'
import vm from 'node:vm'
import { setImmediate } from 'node:timers/promises'
import { MessageChannel, MessagePort } from 'node:worker_threads'

const workerSource = fs.readFileSync(
  new URL('./mockServiceWorker.js', import.meta.url),
  'utf8',
)
const channels: Array<MessageChannel> = []

afterEach(() => {
  for (const channel of channels.splice(0)) {
    channel.port1.close()
    channel.port2.close()
  }
})

function createWorker() {
  const listeners = new Map<string, (event: unknown) => void>()
  const client = {
    id: 'client-id',
    frameType: 'top-level',
    postMessage:
      vi.fn<
        (
          message: { type: string },
          transfer: Array<MessagePort | ArrayBuffer>,
        ) => void
      >(),
  }
  const registration = {
    unregister: vi.fn(async () => true),
  }

  vm.runInNewContext(workerSource, {
    self: {
      clients: {
        get: async () => client,
        matchAll: async () => [client],
      },
      registration,
    },
    addEventListener(type: string, listener: (event: unknown) => void) {
      listeners.set(type, listener)
    },
    MessageChannel: class extends MessageChannel {
      constructor() {
        super()
        channels.push(this)
      }
    },
    crypto,
    Response,
    Headers,
    ReadableStream,
  })

  return {
    client,
    registration,
    dispatchMessage(data: string): Promise<void> {
      let lifetime: Promise<void> | undefined
      listeners.get('message')!({
        data,
        source: client,
        waitUntil(promise: Promise<void>) {
          lifetime = promise
        },
      })

      if (!lifetime) {
        throw new Error('Expected the message event to extend its lifetime')
      }

      return lifetime
    },
    dispatchFetch(request: Request): Promise<Response> {
      let response: Promise<Response> | undefined
      listeners.get('fetch')!({
        request,
        clientId: client.id,
        respondWith(promise: Promise<Response>) {
          response = promise
        },
      })

      if (!response) {
        throw new Error('Expected the worker to intercept the request')
      }

      return response
    },
  }
}

test.each([
  ['KEEPALIVE_REQUEST', { type: 'KEEPALIVE_RESPONSE' }],
  [
    'INTEGRITY_CHECK_REQUEST',
    {
      type: 'INTEGRITY_CHECK_RESPONSE',
      payload: {
        packageVersion: '<PACKAGE_VERSION>',
        checksum: '<INTEGRITY_CHECKSUM>',
      },
    },
  ],
  [
    'MOCK_ACTIVATE',
    {
      type: 'MOCKING_ENABLED',
      payload: { client: { id: 'client-id', frameType: 'top-level' } },
    },
  ],
  ['CLIENT_CLOSE', { type: 'CLIENT_CLOSED' }],
])('settles %s without a MessagePort reply', async (type, notification) => {
  const worker = createWorker()
  const lifetime = worker.dispatchMessage(type)

  // Drain the microtasks from the client lookup and notification dispatch.
  // The client deliberately never replies on the transferred MessagePort.
  const settled = await Promise.race([
    lifetime.then(() => true),
    setImmediate(false),
  ])

  expect(worker.client.postMessage).toHaveBeenCalledWith(notification, [
    expect.anything(),
  ])
  expect(settled).toBe(true)
})

test('keeps CLIENT_CLOSE pending until in-flight requests and unregistration settle', async () => {
  const worker = createWorker()
  const requestPort = Promise.withResolvers<MessagePort>()
  const unregistered = Promise.withResolvers<boolean>()

  worker.client.postMessage.mockImplementation((message, transfer) => {
    if (message.type === 'REQUEST' && transfer[0] instanceof MessagePort) {
      requestPort.resolve(transfer[0])
    }
  })
  worker.registration.unregister.mockReturnValue(unregistered.promise)

  // Activation is a notification, so do not reply on its port either.
  worker.dispatchMessage('MOCK_ACTIVATE')
  await setImmediate()

  const responsePromise = worker.dispatchFetch(
    new Request('https://example.com/user'),
  )
  const port = await requestPort.promise
  const lifetime = worker.dispatchMessage('CLIENT_CLOSE')
  let settled = false
  lifetime.then(() => {
    settled = true
  })

  await setImmediate()
  expect(settled).toBe(false)
  expect(worker.registration.unregister).not.toHaveBeenCalled()
  expect(worker.client.postMessage).not.toHaveBeenCalledWith(
    { type: 'CLIENT_CLOSED' },
    expect.anything(),
  )

  // Unlike notifications, intercepted requests require a reply.
  port.postMessage({
    type: 'MOCK_RESPONSE',
    data: { status: 200, body: 'mocked response' },
  })
  const response = await responsePromise
  await expect(response.text()).resolves.toBe('mocked response')
  expect(settled).toBe(false)
  expect(worker.registration.unregister).toHaveBeenCalledOnce()
  expect(worker.client.postMessage).not.toHaveBeenCalledWith(
    { type: 'CLIENT_CLOSED' },
    expect.anything(),
  )

  unregistered.resolve(true)
  await setImmediate()

  expect(worker.client.postMessage).toHaveBeenCalledWith(
    { type: 'CLIENT_CLOSED' },
    [expect.anything()],
  )
  expect(settled).toBe(true)
})
