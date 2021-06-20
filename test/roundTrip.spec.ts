import {DynamoDB} from '@aws-sdk/client-dynamodb';

interface Event<T = unknown> {
  type: string;
  data: T;
}

interface EventStream<T extends Event = Event> {
  name: string;
  version: number;
  events: () => AsyncGenerator<T>;
}

class EventStore {
  #client: DynamoDB;
  #table: string;

  public constructor(table: string, client: DynamoDB) {
    this.#client = client;
    this.#table = table;
  }

  public async write(stream: string, event: Event) {
    await this.#client.transactWriteItems({
      TransactItems: [
        {
          Update: {
            TableName: this.#table,
            Key: {
              PK: {S: stream},
              SK: {S: '_METADATA'},
            },

            UpdateExpression: 'ADD #version :increment',
            ExpressionAttributeNames: {'#version': 'VERSION'},
            ExpressionAttributeValues: {':increment': {N: '1'}},
          },
        },
        {
          Put: {
            TableName: this.#table,
            Item: {
              PK: {S: stream},
              SK: {S: 'event'},
              TYPE: {S: event.type},
              DATA: {S: JSON.stringify(event.data)},
            },
          },
        },
      ],
    });
  }

  public async read(stream: string) {
    const result = await this.#client.query({
      TableName: this.#table,
      ExpressionAttributeValues: {
        ':stream': {S: stream},
      },
      KeyConditionExpression: 'PK = :stream',
    });

    let version = 0;
    const events: Array<Event> = [];

    for (const item of result.Items || []) {
      if ('VERSION' in item) {
        version = parseInt(item['VERSION'].N || '0');
      } else {
        events.push({
          type: item.TYPE.S || '',
          data: JSON.parse(item.DATA.S || ''),
        });
      }
    }

    return {
      name: stream,
      version,
      events: async function* () {
        for (let i = 0; i < events.length; i++) yield events[i];
      },
    };
  }

  public async createTable() {
    await this.#client.createTable({
      AttributeDefinitions: [
        {AttributeName: 'PK', AttributeType: 'S'},
        {AttributeName: 'SK', AttributeType: 'S'},
      ],
      KeySchema: [
        {AttributeName: 'PK', KeyType: 'HASH'},
        {AttributeName: 'SK', KeyType: 'RANGE'},
      ],
      TableName: this.#table,
      BillingMode: 'PAY_PER_REQUEST',
    });
  }
}

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

describe('When writing to a new stream', () => {
  const streamName = new Date().getMilliseconds().toString();
  const eventData = {
    name: 'SparkleHooves',
    distance: 5,
  };
  let stream: EventStream;

  beforeAll(async () => {
    const store = new EventStore(`table-${streamName}`, local());
    await store.createTable();

    await store.write(streamName, {
      type: 'PonyJumped',
      data: eventData,
    });

    stream = await store.read(streamName);

    console.log(stream);
  });

  it('should be at version 1', () => {
    expect(stream.version).toEqual(1);
  });

  it('should contain the event', async () => {
    let count = 0;
    for await (const e of stream.events()) {
      count++;
      expect(e.data).toMatchObject(eventData);
      expect(e.type).toBe('PonyJumped');
    }

    expect(count).toEqual(1);
  });
});

describe('Test for initial Jest setup.', () => {
  describe('practiceTest', () => {
    test("Given 'Hello World!', return 'Hello World!'", () => {
      const received = 'Hello World!';
      const expected = 'Hello World!';
      expect(received).toBe(expected);
    });
  });
});
