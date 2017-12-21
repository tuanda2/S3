const Events = require('events');

class GcpRequest extends Events {
    constructor(service, operation, params) {
        super();
        this.service = service;
        this.operation = operation;
        this.params = params;
    }

    send(callback) {
        if (callback && typeof callback === 'function') {
            // peform request send with a callback function
            this.operation.call(this.service, this.params, callback);
        } else {
            this.opeartion.call(this.service, this.params, (err, res) => {
                if (err) {
                    this.emit('error', err);
                } else {
                    this.emit('success', res);
                }
                this.emit('complete', { err, res });
            });
        }
        return this;
    }
}

module.exports = GcpRequest;
