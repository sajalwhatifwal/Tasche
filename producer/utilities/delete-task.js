const _ = require('lodash');
const constants = require('../constants');
class DeleteTask {
    constructor(config) {
        this.tascheRedisRepo = new (require('../repository/redis/tasche'))(config);
    }

    async process(options) {
        const taskId = _.get(options, 'taskId', null);
        let [taskErr, taskQueue] = await constants.general.invoker(this.tascheRedisRepo.getKeyFromHash('{Task}', taskId, 'queue'));
        if(taskErr) {
            return Promise.reject({
                type: 'FAILED',
                msg: _.get(taskErr, 'message', null),
            });
        }
        if(!taskQueue) {
            return Promise.reject({
                type: 'FAILED',
                msg: 'No such task exists!'
            });
        }
        let [remErr, removed] = await constants.general.invoker(this.tascheRedisRepo.removeFromSet('{TaskQueue}', taskQueue, taskId));
        if(remErr) {
            return Promise.reject({
                type: 'FAILED',
                msg: _.get(remErr, 'message', null)
            });
        }
        this.tascheRedisRepo.deleteKey('{Task}', taskId);
        return Promise.resolve({
            type: 'SUCCESS',
            msg: !removed ? 'Task already executed!' : 'Task deleted successfully!'
        });
    }
}

module.exports = DeleteTask;