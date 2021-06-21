import {DynamoDB} from '@aws-sdk/client-dynamodb';
import {EventStore, EventStream, Version} from '../src';
import ksuid = require('ksuid');

const local = () => {
  return new DynamoDB({
    endpoint: 'http://127.0.0.1:8000',
    region: 'eu-west-1',
    credentials: {
      accessKeyId: 'fake-id',
      secretAccessKey: 'fake-secret',
    },
  });
};

const random_stream = () => ksuid.randomSync().string;

const given_an_empty_event_store = async () => {
  const name = random_stream();
  console.log(`creating table-${name}`);
  const store = new EventStore<PonyJumped>(`table-${name}`, local());
  await store.createTable();
  return store;
};

const copy = async (g: AsyncGenerator<PonyJumped>) => {
  const result: Array<PonyJumped> = [];
  for await (const e of g) {
    result.push(e);
  }
  return result;
};

interface PonyJumped {
  type: 'PonyJumped';
  data: {
    name: string;
    distance: number;
  };
}

const jump = (name: string, distance: number) => ({
  type: 'PonyJumped' as const,
  data: {
    name,
    distance,
  },
});

describe('When writing to a new stream', () => {
  const streamName = random_stream();
  const event = jump('SparkleHooves', 5);
  let stream: EventStream<PonyJumped>;

  beforeAll(async () => {
    const store = await given_an_empty_event_store();
    await store.write(streamName, event);

    stream = await store.read(streamName);
  });

  it('should be at version 1', () => {
    expect(stream.version).toEqual(1);
  });

  it('should contain the event', async () => {
    for await (const e of stream.events()) {
      expect(e.data).toMatchObject(event.data);
      expect(e.type).toBe('PonyJumped');
    }
  });
});

describe('When writing to an existing stream', () => {
  const streamName = random_stream();
  let stream: EventStream<PonyJumped>;
  let events: Array<PonyJumped>;

  beforeAll(async () => {
    const store = await given_an_empty_event_store();
    await store.write(streamName, jump('SparkleHooves', 5));
    await store.write(streamName, jump('DerpyHooves', 5));

    stream = await store.read(streamName);
    events = await copy(stream.events());
  });

  it('should be at version 2', () => {
    expect(stream.version).toEqual(2);
  });

  it('should contain two events', async () => {
    expect(events.length).toBe(2);
  });

  it('should have the correct ordering', () => {
    expect(events[0].data.name).toBe('SparkleHooves');
    expect(events[1].data.name).toBe('DerpyHooves');
  });
});

describe('When using version control on an existing stream', () => {
  const streamName = random_stream();
  let store: EventStore<PonyJumped>;

  beforeAll(async () => {
    store = await given_an_empty_event_store();
    await store.write(streamName, jump('FirstHooves', 1));
    await store.write(streamName, jump('SecondHooves', 2));
  });

  it('should permit writes for the correct version', async () => {
    await store.write(streamName, jump('PresentHooves', 2));
  });

  it('should prevent writes for an earlier version', async () => {
    await expect(
      store.write(streamName, jump('PastHooves', 1), 1)
    ).rejects.toThrow();
  });

  it('should prevent writes for a future version', async () => {
    await expect(
      store.write(streamName, jump('FutureHooves', 1), 9)
    ).rejects.toThrow();

  });

  it('should prevent writes expecting a new stream', async () => {
    await expect(
      store.write(streamName, jump('EmptyHooves', 1), Version.None)
    ).rejects.toThrow();
  });
});
