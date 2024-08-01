const _ = require('lodash');
const momentTz = require('moment-timezone');
const uuid = require('uuid');
const constants = require('../constants');
class CreateTask {
    constructor(config) {
        this.tascheRedisRepo = new (require('../repository/redis/tasche'))(config);
    }

    async process(options) {
        let { type, meta, triggerAtEpoch } = options;
        if(_.isNil(triggerAtEpoch)) {
            triggerAtEpoch = _.ceil(Date.now() / 1000);
        }
        triggerAtEpoch = triggerAtEpoch * 1000;
        const taskId = uuid.v4();
        const secondInThatDay = momentTz(triggerAtEpoch).tz('Asia/Kolkata').diff(momentTz(triggerAtEpoch).tz('Asia/Kolkata').startOf('day'), 'seconds');
        const thatDay = momentTz(triggerAtEpoch).tz('Asia/Kolkata').format('YYYY-MM-DD');
        const queue = `${thatDay}_${secondInThatDay}`;
        try {
            let [validErr, validMessage] = this.validateInput({ type, meta });
            if(validErr) {
                throw {
                    type: 'INVALID_INPUT',
                    message: validMessage
                };
            }
            const transformedObject = this.transformInput({ taskId, meta });
            meta = _.get(transformedObject, 'meta', {});
            const taskMeta = ['meta', JSON.stringify(meta), 'queue', queue];
            await this.tascheRedisRepo.setKeysInHashWithExpiryAtTimeOrNot('{Task}', taskId, taskMeta, false);
            await this.tascheRedisRepo.addToSet('{TaskQueue}', queue, taskId);
            const [err, res] = await constants.general.invoker(this.tascheRedisRepo.addToSortedSetWithExpiryAtTime('{TasksToBeDispatched}', thatDay, secondInThatDay, secondInThatDay, false));
            if(err) {
                this.tascheRedisRepo.removeFromSet('{TaskQueue}', queue, taskId);
                throw {
                    type: 'FAILED',
                    message: err.message
                };
            }
            return Promise.resolve({
                taskId,
                type: 'SUCCESS',
                msg: 'Task created successfully!'
            });
        }
        catch (error) {
            return Promise.reject({
                type: error.type || 'FAILED',
                msg: error.message || 'Could not create task, please try again!'
            });
        }
    }

    validateInput({ type, meta }) {
        if(_.includes(constants.general.supportedTypes, type)) {
            for(let key of constants.general.types[type].requiredMeta) {
                if(!_.has(meta, key)) {
                    return [true, 'Required fields missing inside meta'];
                }
            }
            return [false, null];
        }
        return [true, 'Type not supported'];
    }

    transformInput({ taskId, meta }) {
        // 1. Adding taskId as a queryParam for tracking purposes
        let url = _.get(meta, 'url', '');
        url += `${_.includes(url, '?') ? '&' : '?'}cloudTaskId=${taskId}`;
        _.set(meta, 'url', url);
        return { meta };
    }
}

module.exports = CreateTask;