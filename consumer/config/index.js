module.exports = {
  activeEnv: 'PRODUCTION',
  tascheRedis: {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASS
  }
};
