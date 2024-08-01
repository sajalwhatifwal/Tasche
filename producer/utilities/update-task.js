const _ = require('lodash');
const momentTz = require('moment-timezone');
const constants = require('../constants');

class UpdateTask {
    constructor(config) {
        this.tascheRedisRepo = new (require('../repository/redis/tasche'))(config);
    }
    
    async process(options) {
        let { taskId, meta, triggerAtEpoch } = options;
        let updatedTaskMeta = {}, newQueue = null;
        try {
            let [taskErr, taskMeta] = await constants.general.invoker(this.tascheRedisRepo.getWholeHash('{Task}', taskId));
            if(taskErr) {
                throw {
                    type: 'TASK_NOT_FOUND',
                    message: taskErr.message
                };
            }
            if(!_.size(taskMeta)) {
                throw {
                    type: 'TASK_NOT_FOUND',
                    message: 'No such task exists!'
                };
            }
            let previousMeta = taskMeta.meta;
            let previousQueue = taskMeta.queue;
            previousMeta = JSON.parse(previousMeta);
            if(triggerAtEpoch) {
                triggerAtEpoch = triggerAtEpoch * 1000;
                const secondInThatDay = momentTz(triggerAtEpoch).tz("Asia/Kolkata").diff(momentTz(triggerAtEpoch).tz('Asia/Kolkata').startOf('day'), 'seconds');
                const thatDay = momentTz(triggerAtEpoch).tz('Asia/Kolkata').format('YYYY-MM-DD');
                newQueue = `${thatDay}_${secondInThatDay}`;
                let [switchErr, switched] = await constants.general.invoker(this.tascheRedisRepo.switchBetweenSets(`{TaskQueue}_${previousQueue}`, `{TaskQueue}_${newQueue}`, taskId));
                if(switchErr) {
                    throw {
                        type: 'FAILED',
                        message: 'Could not schedule the task for the desired epoch, please try again!'
                    };
                }
                _.set(updatedTaskMeta, 'queue', newQueue);
            }
            if(meta) {
                for(let key in meta) {
                    _.set(previousMeta, key, meta[key]);
                }
                _.set(updatedTaskMeta, 'meta', JSON.stringify(previousMeta));
            }
            let newTaskMeta = [];
            for(let key in updatedTaskMeta) {
                newTaskMeta.push(key, updatedTaskMeta[key]);
            }
            if(_.size(newTaskMeta)) {
                let [updateErr, updated] = await constants.general.invoker(this.tascheRedisRepo.setKeysInHashWithExpiryAtTimeOrNot('{Task}', taskId, newTaskMeta, false));
                if(updateErr) {
                    throw {
                        type: 'FAILED',
                        message: 'Error in updating task meta, please try again!'
                    };
                }
            }
            return Promise.resolve({
                taskId,
                type: 'SUCCESS',
                msg: 'Task updated successfully!'
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
        if(!_.isNil(type) && _.includes(constants.general.supportedTypes, type)) {
            for(let key of constants.general.types[type].requiredMeta) {
                if(!_.has(meta, key)) {
                    return [true, 'Required fields missing inside meta'];
                }
            }
            return [false, null];
        }
        return [true, 'Type not supported'];
    }
}

module.exports = UpdateTask;