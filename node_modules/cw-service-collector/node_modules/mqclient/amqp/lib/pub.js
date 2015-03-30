var amqp = require('amqplib')

module.exports = function () {

  // keep a reference of all the exchanges here,
  var exchanges = {}

  return function (exchange, message, callback) {

    // already connected
    if ((exchange in exchanges)) {

      exchanges[exchange].publish(exchange, '', new Buffer(message))
      return callback && callback(null, message)

    }
    // first request, connect first
    else {

      amqp.connect('amqp://localhost').then(function(conn) {

        conn.createChannel().then(function(ch) {

          var ok = ch.assertExchange(exchange, 'fanout', {
            durable: false
          }).then(function () {

            exchanges[exchange] = ch
            exchanges[exchange].publish(exchange, '', new Buffer(message))
            return callback && callback(null, message)

          }, function (fail) {
            return callback && callback(fail, message)
          })

        })

      }).then(null, callback)

    }

  }

}


