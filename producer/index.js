
const config = require('./config');
class TascheProducer {
    constructor() {
        this.config = config;
        this.createTask = new (require('./utilities/create-task'))(this.config);
        this.deleteTask = new (require('./utilities/delete-task'))(this.config);
        this.updateTask = new (require('./utilities/update-task'))(this.config);
    }

    async create(options) {
        return this.createTask.process(options);
    }

    async delete(options) {
        return this.deleteTask.process(options);
    }

    async update(options) {
        return this.updateTask.process(options);
    }
}

module.exports = TascheProducer;