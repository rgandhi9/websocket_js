// https://coincheck.com/documents/exchange/api#websocket-trades
const WebSocket = require('websocket').w3cwebsocket;
// npm install --save @google-cloud/pubsub
const {PubSub} = require('@google-cloud/pubsub');

const topicNameOrId = 'coincheck_topic';
const pubSubClient = new PubSub();

const schema = [
    "timestamp",
    "transaction_id",
    "pair",
    "transaction_rate",
    "transaction_amount",
    "order_side",
    "taker_id",
    "maker_id"];

async function publishMessage(topicNameOrId, data) {
    // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
    const dataBuffer = Buffer.from(data);

    try {
        const messageId = await pubSubClient
            .topic(topicNameOrId)
            .publishMessage({data: dataBuffer});
        console.log(`Message ${messageId} published.`);
    } catch (error) {
        console.error(`Received error while publishing: ${error.message}`);
        process.exitCode = 1;
    }
}

function connect() {
    const socket = new WebSocket("wss://ws-api.coincheck.com/");
    
    socket.onopen = function(event) {
        console.log("Connection established");
        socket.send(JSON.stringify({type: "subscribe", channel: "btc_jpy-trades"}))
    };

    socket.onmessage = function(event) {
        for (let i = 0; i < JSON.parse(event.data).length; i++) {
            object = schema.reduce((acc, schema, index) => {
                acc[schema] = JSON.parse(event.data)[i][index];
                return acc;
            }, {});
            publishMessage(topicNameOrId, JSON.stringify(object));
            console.log(JSON.stringify(object));
        }
    };

    socket.onerror = function(error) {
        console.error("WebSocket error:", error);
    };

    socket.onclose = function(event) {
        console.log("Connection closed - Reconnecting...");
        setTimeout(function(){connect()}, 2000);
    };
}

connect();
