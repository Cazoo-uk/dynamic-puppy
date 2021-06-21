import {DynamoDB, Update} from '@aws-sdk/client-dynamodb';
import ksuid = require('ksuid');

export interface Event<T = unknown> {
  type: string;
  data: T;
}

export interface EventStream<T extends Event = Event> {
  name: string;
  version: number;
  events: () => AsyncGenerator<T>;
}

export enum Version {
  Any = -1,
  None = -2,
}

const metadataUpdate = (
  table: string,
  stream: string,
  expectedVersion: Version | number
) => {
  const update: Update = {
    TableName: table,
    Key: {
      PK: {S: stream},
      SK: {S: '_METADATA'},
    },

    UpdateExpression: 'ADD #version :increment',
    ExpressionAttributeNames: {'#version': 'VERSION'},
    ExpressionAttributeValues: {':increment': {N: '1'}},
  };

  if (expectedVersion === Version.None)
    update.ConditionExpression = 'attribute_not_exists(PK)';

  if (expectedVersion > -1 && update.ExpressionAttributeValues) {
    update.ConditionExpression = '#version = :expected';
    update.ExpressionAttributeValues[':expected'] = {
      N: expectedVersion.toString(),
    };
  }

  return {Update: update};
};

function eventInsert<TEvent extends Event = Event>(
  table: string,
  stream: string,
  event: TEvent
) {
  const id = ksuid.randomSync().string;
  return {
    Put: {
      TableName: table,
      Item: {
        PK: {S: stream},
        SK: {S: `EVENT-${id}`},
        TYPE: {S: event.type},
        DATA: {S: JSON.stringify(event.data)},
      },
    },
  };
}

export class EventStore<TEvent extends Event = Event> {
  #client: DynamoDB;
  #table: string;

  public constructor(table: string, client: DynamoDB) {
    this.#client = client;
    this.#table = table;
  }

  public async write(
    stream: string,
    event: TEvent,
    expectedVersion: Version | number = Version.Any
  ) {
    await this.#client.transactWriteItems({
      TransactItems: [
        metadataUpdate(this.#table, stream, expectedVersion),
        eventInsert(this.#table, stream, event),
      ],
    });
  }

  public async read(stream: string): Promise<EventStream<TEvent>> {
    const result = await this.#client.query({
      TableName: this.#table,
      ExpressionAttributeValues: {
        ':stream': {S: stream},
      },
      KeyConditionExpression: 'PK = :stream',
    });

    console.log(JSON.stringify(result, null, 2));

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
        for (let i = 0; i < events.length; i++) yield events[i] as TEvent;
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
