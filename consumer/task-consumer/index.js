const cronScheduler = require('node-cron');
const _ = require('lodash');
const momentTz = require('moment-timezone');
const config = require('../config');
const constants = require('../constants');
const request = require('request');
const TascheRedisRepo = require('../repository/data/tascheRedisRepo');
const tascheRedisRepo = new TascheRedisRepo(config);
let cleanUpLastRecordedSecond = -1, lastDelayInMilliseconds = 0, maximumCronDelayError = 0;

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runSkipEnhancement(currentDelayInMilliseconds, elevenSecondslaterQueue, proposedDelayTime) {
    // Write a full write-up
    const delayDifference = lastDelayInMilliseconds - currentDelayInMilliseconds;
    maximumCronDelayError = _.max([maximumCronDelayError, delayDifference]);
    lastDelayInMilliseconds = currentDelayInMilliseconds;
    console.log({
        currentDelayInMilliseconds, elevenSecondslaterQueue, proposedDelayTime, maximumCronDelayError
    });
    if(currentDelayInMilliseconds < 2 * maximumCronDelayError) {
        fakeCronConsumer(elevenSecondslaterQueue, proposedDelayTime);
    }
}

async function fakeCronConsumer(queue, time) {
    await sleep(time);
    await pullAndDispatch(queue, 'FAKE_CRON');
}

async function actualCronConsumer(queue, elevenSecondslaterQueue) {
    console.log("Master Start", Date.now());
    const extraMillisecond = momentTz().tz("Asia/Kolkata").millisecond();
    console.log(1000-extraMillisecond, 11005 - extraMillisecond);
    runSkipEnhancement(1000-extraMillisecond, elevenSecondslaterQueue, 11005 - extraMillisecond);
    await sleep(1005-extraMillisecond);
    await pullAndDispatch(queue, 'ACTUAL_CRON');
    console.log("Master End", Date.now());
    return Promise.resolve();
}

function fireTask(parsedTask) {
    return new Promise((resolve, reject) => {
        const { method, headers, url, body } = parsedTask;
        const options = {
            headers,
            url,
            method,
            json: body || true
        };
        request(options, (err, response, body) => {
            if (err) {
                return reject(err);
            }
            if (response.statusCode != 200) {
                return reject(response.statusCode);
            }
            return resolve(body);
        });
    });
}

async function cleanUpStreamConsumer() {
    let secondsToday, today, yesterday, cleanErr, cleaned;
    while(true) {
        secondsToday = momentTz().tz("Asia/Kolkata").diff(momentTz().tz('Asia/Kolkata').startOf('day'), 'seconds');
        if(secondsToday < cleanUpLastRecordedSecond) { // This will happen only when the server jumps to the next day
            secondsToday = secondsToday + cleanUpLastRecordedSecond + 1;
            yesterday = momentTz().tz('Asia/Kolkata').subtract(1, 'day').format('YYYY-MM-DD');
            [cleanErr, cleaned] = await constants.general.invoker(cleanSortedSet(yesterday, secondsToday));
            console.log(cleanErr, cleaned, secondsToday, cleanUpLastRecordedSecond, yesterday);
            if(cleanErr) {
                continue;
            }
            secondsToday = secondsToday - cleanUpLastRecordedSecond - 1;
            cleanUpLastRecordedSecond = 0;
        }
        if(secondsToday > cleanUpLastRecordedSecond) {
            today = momentTz().tz('Asia/Kolkata').format('YYYY-MM-DD');
            [cleanErr, cleaned] = await constants.general.invoker(cleanSortedSet(today, secondsToday));
            console.log(cleanErr, cleaned, secondsToday, cleanUpLastRecordedSecond, today);
            if(cleanErr) {
                continue;
            }
            cleanUpLastRecordedSecond = secondsToday;
        }
    }
}

async function cleanSortedSet(day, seconds) {
    console.log(day, seconds);
    const [err, leftSeconds] = await constants.general.invoker(tascheRedisRepo.getFromSortedSetOnScore('{TasksToBeDispatched}', day, 0, seconds - 1));
    if(err) {
        return Promise.resolve();
    }
    if(_.size(leftSeconds)) {
        console.log(leftSeconds, seconds);
    }
    for(let second of leftSeconds) {
        await pullAndDispatch(`${day}_${second}`, 'CLEAN_UP');
    }
    return Promise.resolve();
}

async function pullAndDispatch(queue, source) {
    console.log({
        queue, source
    });
    const _internalPullAndDispatch = async (queue) => {
        let [pullErr, pulledTaskId] = await constants.general.invoker(tascheRedisRepo.popFromSet('{TaskQueue}', queue));
        if(pullErr) {
            return Promise.resolve(false);
        }
        if(_.isNil(pulledTaskId)) {
            const [today, secondsToday] = _.split(queue, '_');
            tascheRedisRepo.removeFromSortedSet('{TasksToBeDispatched}', today, secondsToday);
            return Promise.resolve(true);
        }
        tascheRedisRepo.getKeyFromHash('{Task}', pulledTaskId, 'meta').then((stringifiedTask) => {
            const parsedTask = JSON.parse(stringifiedTask);
            tascheRedisRepo.deleteKey('{Task}', pulledTaskId);
            fireTask(parsedTask).then((reply) => {
                tascheRedisRepo.deleteKey('{Task}', pulledTaskId);
            }).catch((err) => {
                console.log(err);
            });
        }).catch((taskErr) => {
            tascheRedisRepo.addToSet('{TaskQueue}', queue, pulledTaskId).catch((err) => {});
        });
        return Promise.resolve(false);
    }
    while(true) {
        console.log("Start", Date.now());
        const returnList = await Promise.all([_internalPullAndDispatch(queue), _internalPullAndDispatch(queue), _internalPullAndDispatch(queue), _internalPullAndDispatch(queue), _internalPullAndDispatch(queue), _internalPullAndDispatch(queue), _internalPullAndDispatch(queue), _internalPullAndDispatch(queue), _internalPullAndDispatch(queue), _internalPullAndDispatch(queue)]);
        console.log("End", Date.now(), returnList);
        if(_.includes(returnList, true)) {
            return Promise.resolve();
        }
    }
}

function main() {
    const isCleanUp = process.env.JOB_TYPE === 'CLEAN_UP';
    if(isCleanUp) {
        cleanUpStreamConsumer();
    }
    else {
        const start = _.toInteger(process.env.INITIAL_START);
        cronScheduler.schedule(`${start},${10 + start},${20 + start},${30 + start},${40 + start},${50 + start} * * * * *`, async function () {
            let secondsToday = momentTz().tz("Asia/Kolkata").add(1, 'seconds').diff(momentTz().tz('Asia/Kolkata').startOf('day'), 'seconds');
            let today = momentTz().tz('Asia/Kolkata').format('YYYY-MM-DD');
            if(secondsToday == 86400) {
                secondsToday = 0;
                today = momentTz().tz('Asia/Kolkata').add(5, 'seconds').format('YYYY-MM-DD');
            }
            let elevenSecondsLater = momentTz().tz("Asia/Kolkata").add(11, 'seconds').diff(momentTz().tz('Asia/Kolkata').startOf('day'), 'seconds');
            let elevenSecondsLaterToday = momentTz().tz('Asia/Kolkata').format('YYYY-MM-DD');
            if(elevenSecondsLater >= 86400) { // can be moved to common function in future revamps
                elevenSecondsLater = elevenSecondsLater - 86400;
                elevenSecondsLaterToday = momentTz().tz('Asia/Kolkata').add(50, 'seconds').format('YYYY-MM-DD');
            }
            console.log(`${today}_${secondsToday}`, `${elevenSecondsLaterToday}_${elevenSecondsLater}`);
            await actualCronConsumer(`${today}_${secondsToday}`, `${elevenSecondsLaterToday}_${elevenSecondsLater}`);
        }, { timezone: "Asia/Kolkata" });
    }
}

main();
