const Redis = require('ioredis');
const RedisTimeout = require('ioredis-timeout');
const _ = require('lodash');

class TascheRedisRepo {
	constructor(config) {
		this.redisClient = new Redis({
			port: config.tascheRedis.port,
			host: config.tascheRedis.host,
			password: config.tascheRedis.password
		});
		RedisTimeout(this.redisClient, config.tascheRedis.timeout);
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

	setKeysInHashWithExpiryAtTimeOrNot(header, key, values, time) {
		return new Promise((resolve, reject) => {
			let self = this;
			this.redisClient.hmset(
				header + '_' + key,
				values,
				function (err, reply) {
					if (err) {
						return reject(err);
					}
					if (time) {
						self.redisClient.expireat(
							header + '_' + key,
							time,
							function (error, response) {
								if (error) {
									return reject(error);
								}
								return resolve(response);
							}
						);
					}
					return resolve();
				}
			);
		});
	}

	addToSortedSetWithExpiryAtTime(header, key, score, value, time) {
		return new Promise((resolve, reject) => {
			let self = this;
			this.redisClient.zadd(
				header + '_' + key,
				score,
				value,
				function (err, reply) {
					if (err) {
						return reject(err);
					}
					if (time) {
						self.redisClient.expireat(
							header + '_' + key,
							time,
							function (error, response) {
								if (error) {
									return reject(error);
								}
								return resolve(response);
							}
						);
					}
					return resolve(reply);
				}
			);
		});
	}

	removeFromSet(header, key, value) {
		return new Promise((resolve, reject) => {
			this.redisClient.srem(
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

	switchBetweenSets(sourceSet, destinationSet, member) {
		return new Promise((resolve, reject) => {
			this.redisClient.smove(sourceSet, destinationSet, member, (err, reply) => {
				if (err) {
					return reject(err);
				}
				return resolve(reply);
			});
		});
	}

	getWholeHash(header, key) {
		return new Promise((resolve, reject) => {
			this.redisClient.hgetall(header + '_' + key, (err, reply) => {
				if (err) {
					return reject(err);
				}
				return resolve(reply);
			});
		});
	}
}

module.exports = TascheRedisRepo;
