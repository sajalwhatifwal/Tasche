const Redis = require('ioredis');
const _ = require('lodash');

class TascheRedisRepo {
    constructor(config) {
        const envConfig = config;
        this.redisClient = new Redis({
            port: envConfig.tascheRedis.port,
            host: envConfig.tascheRedis.host,
            password: envConfig.tascheRedis.password
        });
    }
    
	hgetall(key) {
		return new Promise((resolve, reject) => {
			this.redisClient.hgetall(
				key,
				function (err, reply) {
					if (err) {
						return reject(err);
					}
					return resolve(reply);
				}
			);
		});
	}

    popFromSet(header, key) {
        return new Promise((resolve, reject) => {
            this.redisClient.spop(header + '_' + key, function (err, reply) {
                if (err) {
                    return reject(err);
                }
                return resolve(reply);
            })
        })
    }

    getKeyFromHash(header, key, value) {
		return new Promise((resolve, reject) => {
			this.redisClient.hget(
				header + '_' + key,
				value,
				function (err, reply) {
					if (err) {
						return reject(err);
					}
					return resolve(reply);
				}
			);
		});
	}

    deleteKey(header, key) {
		return new Promise((resolve, reject) => {
			this.redisClient.del(header + '_' + key, function (err, reply) {
				if (err) {
					return reject(err);
				}
				return resolve(reply);
			});
		});
	}

    addToSet(header, key, value) {
		return new Promise((resolve, reject) => {
			this.redisClient.sadd(
				header + '_' + key,
				value,
				function (err, reply) {
					if (err) {
						return reject(err);
					}
					return resolve(reply);
				}
			);
		});
	}

	removeFromSortedSet(header, key, value) {
		return new Promise((resolve, reject) => {
			this.redisClient.zrem(
				header + '_' + key,
				value,
				function (err, reply) {
					if (err) {
						return reject(err);
					}
					return resolve(reply);
				}
			);
		});
	}
	
	setKeyInHash(header, key, field, value) {
		return new Promise((resolve, reject) => {
			this.redisClient.hset(header + '_' + key, field, value, function (err, reply) {
				if (err) {
					return reject(err);
				}
				return resolve(reply);
			});
		});
	}

	getFromSortedSetOnScore(header, key, start, end) {
		return new Promise((resolve, reject) => {
			this.redisClient.zrangebyscore(header + '_' + key, start, end, function (err, reply) {
				if (err) {
					return reject(err);
				}
				return resolve(reply);
			});
		});
	}

	incrementHashKeyByValue(header, key, value) {
		return new Promise((resolve, reject) => {
			this.redisClient.hincrby(header, key, value, function (err, reply) {
				if (err) {
					return reject(err);
				}
				return resolve(reply);
			});
		});
	}
}

module.exports = TascheRedisRepo;