const { expect, test } = require('@jest/globals')
const { transformEventToRow, exportEvents } = require('../index')

test('vaildate unmarshal of transform event to row', () => {
    const jsonObject = {
        event: '$autocapture',
        properties: { props: 'is awesome 1998', $elements: { awrsomfseds: 'awesome' } },
        $set: 'set',
        $set_once: 'setonce',
        distinct_id: 'distinct id',
        team_id: 'team id',
        site_url: 'site url',
        now: 'now',
        sent_at: 'sent at',
        uuid: 'uuid',
    }

    const response = transformEventToRow(jsonObject)
    expect(response.event).toEqual('$autocapture')
    expect(response.distinct_id).toEqual('distinct id')
    expect(response.team_id).toEqual('team id')
    expect(response.site_url).toEqual('site url')
    expect(response.uuid).toEqual('uuid')
})

test('export event and generate array for csv', async () => {
    const global = {
        eventsToIgnore: ['$autocapture'],
        tests: true,
    }
    const events = ['$autocapture', '$pageview']
    const storageObject = {}
    const mockedStorage = {
        get: (key, none) => storageObject[key] || none,
        set: (key, value) => {
            storageObject[key] = value
        },
    }
    const result = await exportEvents(events, { global, mockedStorage })
    expect(result.length).toEqual(1)
})
