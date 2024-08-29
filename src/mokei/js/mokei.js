/**
 * Version 0.2.10
 * Websocket handler class for use with Python Mokei backend.
 *
 * Preferred usage is to instantiate from mokeiWebsocketExchange to avoid duplicates.
 *
 * const ws = mokeiWebsocketExchange.getWebsocket('wss://some.url.com/ws');
 * ws.on('eventName', (data) => console.log(data));  // register callback for event
 * ws.ontext((text) => console.log(text));  // register callback for text
 *
 * ws.connect();  // connect for first time.  reconnection is automatic if disconnected
 *
 * ws.sendText('someText'); // send text to the backend
 * ws.sendEvent('someEvent', {key1: 'value1', key2: 'value2'});  // send an event
 *
 */

const MEM = 'μοκιε';  // mokeiEventMarker - marks start of events rather than text


export class MokeiWebSocket {
    /**
     * Websocket handler class for use with Python Mokei backend.
     * @param url - starts with ws:// or wss://
     */
    constructor(url) {
        this.url = url;
        this._connectCalled = false;
        this._onconnect = [];
        this._ondisconnect = [];
        this._ontext = [];
        this._onbinary = [];
        this._onevent = {};
        this.connected = false;
        this._messages = [];
        this._pendingSend = false;
        this.defaultBackoff = 100;
        this.maxBackoff = 10000;
        this._backoff = this.defaultBackoff;
        this._ws = null;
    }

    _handleMessage(message) {
        if (message.slice(0, MEM.length) === MEM) {
            const eventObj = JSON.parse(message.slice(MEM.length));
            if (eventObj.hasOwnProperty('event') && eventObj.hasOwnProperty('data')) {
                const eventType = eventObj.event;
                const eventData = eventObj.data;
                if (this._onevent.hasOwnProperty(eventType)) {
                    this._onevent[eventType].forEach(handler => handler(eventData));
                }
            }
        } else {
            this._ontext.forEach(handler => handler(message));
        }
    }

    _sendAll() {
        if (!this.connected) {
            setTimeout(this._sendAll, 1000);
            return;
        }
        while (this._messages.length) {
            this._ws.send(this._messages.shift());
        }
        this._pendingSend = false;
    }

    sendText(msg) {
        this._messages.push(msg);
        if (!this._pendingSend) {
            this._pendingSend = true;
            this._sendAll();
        }
    }

    sendEvent(event, data) {
        const fullData = {
            'event': event,
            'data': data,
        }
        this.sendText(MEM + JSON.stringify(fullData));
    }


    onconnect(callback) {
        this._onconnect.push(callback);
    }

    ondisconnect(callback) {
        this._ondisconnect.push(callback);
    }

    ontext(callback) {
        this._ontext.push(callback);
    }

    on(event, callback) {
        if (!this._onevent.hasOwnProperty(event)) {
            this._onevent[event] = [];
        }
        this._onevent[event].push(callback);
    }

    connect() {
        /**
         * Must be called once to create and connect the websocket.
         * Event callbacks may be registered before or after connect() is called,
         * but some events may be missed if registering after calling connect()
         */
        if (!this._connectCalled) {
            this._connectCalled = true;
            this._connect();
        }
    }

    _connect() {
        /**
         * Internal method to handle actually creating and connecting to the websocket
         * This is called only once by this.connect (and not called on subsequent calls to this.connect)
         * This is also called again when a disconnect occurs, after a given back-off
         */
        this._ws = new WebSocket(this.url);
        this._ws.onopen = () => {
            this.connected = true;
            this._backoff = this.defaultBackoff;
            this._sendAll();
            this._onconnect.forEach(handler => handler());
        }
        this._ws.onmessage = ((event) => this._handleMessage(event.data));
        this._ws.onclose = () => {
            if (this.connected) {
                this._ondisconnect.forEach(handler => handler());
            }
            this.connected = false;
            setTimeout(this._connect.bind(this), this._backoff);
            this._backoff = Math.min(this._backoff * 2 + Math.random() * 1000, this.maxBackoff);
        }
    }
}

class MokeiSocketExchange {
    constructor() {
        this.websockets = {};
    }

    getWebSocket(url) {
        if (!this.websockets.hasOwnProperty(url)) {
            this.websockets[url] = new MokeiWebSocket(url);
        }
        return this.websockets[url];
    }

}


// import and use mokeiSocketExchange to get websockets
export const mokeiSocketExchange = new MokeiSocketExchange();
