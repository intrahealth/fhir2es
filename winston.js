const winston = require('winston');


let transports = [ new winston.transports.Console() ]
const logger = winston.createLogger({
  transports: transports,
  format: winston.format.combine(
    winston.format.colorize({
      all: true,
    }),
    winston.format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss',
    }),
    winston.format.simple()
  ),
});
module.exports = logger;