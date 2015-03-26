'use_strict'

module.exports= Bus

var events= require('events')
var redis= require('redis')
var uuid= require('uuid')



function Bus(name, options) {

    if (!(this instanceof Bus)) {
        return new Bus(name, options)
    }

    this.name= name || 'bus'

    this.announceChannelName= this.name
    this.announceChannelPattern= [ this.announceChannelName, '*' ].join('::')
    this.announceKeyName= this.name

    this.successChannelName= [ this.name, 'success' ].join(':')
    this.successChannelPattern= [ this.successChannelName, '*' ].join('::')
    this.successKeyName= [ this.name, 'success' ].join(':')

    this.errorChannelName= [ this.name, 'error' ].join(':')
    this.errorChannelPattern= [ this.errorChannelName, '*' ].join('::')
    this.errorKeyName= [ this.name, 'error' ].join(':')

    this.taskIdx= {}

    this.pub= redis.createClient()
    this.sub= redis.createClient()

    this._events= new (events.EventEmitter)

    this.sub.psubscribe( this.announceChannelPattern, this.successChannelPattern, this.errorChannelPattern, function (err) {
        if (err) {
            this.emit(Error('WTF?')) // @todo
            return
        }
        this.emit('ready')
        this.sub.on('pmessage', this._onMessage.bind(this))
    }.bind(this))

}

Object.setPrototypeOf(Bus.prototype, events.EventEmitter.prototype)



Bus.prototype._onMessage= function (pattern, channel, message) {
    var id= message
    if (pattern === this.announceChannelPattern) {
        if (!(this.taskIdx[id])) {
            var name= channel.substring( (this.name+'::').length )
            this._getMessage(this.announceKeyName, id, function (message) {
                if (message) {
                    message= JSON.parse(message)
                    this._events.emit(name, { id:id, message:message })
                }
            }.bind(this))
        }
        return
    }
    if (pattern === this.successChannelPattern) {
        var task= this.taskIdx[id]
        if (task) {
            this._getMessage(this.successKeyName, id, function (message) {
                if (message) {
                    message= JSON.parse(message)
                }
                task.callback(null, message)
                delete this.taskIdx[id]
            }.bind(this))
        }
        return
    }
    if (pattern === this.errorChannelPattern) {
        var task= this.taskIdx[id]
        if (task) {
            this._getMessage(this.errorKeyName, id, function (message) {
                if (message) {
                    message= JSON.parse(message)
                }
                task.callback(message)
                delete this.taskIdx[id]
            }.bind(this))
        }
        return
    }
}

Bus.prototype._getMessage= function (key, id, callback) {
    key= [ key, id ].join('::')
    this.pub.rpop(key, function (err, message) {
        if (err) {
            this.emit(Error('WTF?')) // @todo
            return
        }
        callback(message)
    })
}

Bus.prototype._setMessage= function (key, id, message, callback) {
    message= JSON.stringify(message)
    key= [ key, id ].join('::')
    this.pub.rpush(key, message, function (err) {
        if (err) {
            this.emit(Error('WTF?')) // @todo
            return
        }
        this.pub.expire(key, 30, function (err) {
            if (err) {
                this.emit(Error('WTF?')) // @todo
                return
            }
            callback()
        }.bind(this))
    }.bind(this))
}



Bus.prototype.handle= function(name, message, callback) {
    message= JSON.stringify(message)
    var id= uuid.v1()
    this._setMessage(this.announceKeyName, id, message, function () {
        this.taskIdx[id]= {
            callback: callback
        }
        var key= [ this.announceChannelName, name ].join('::')
        this.pub.publish(key, id)
    }.bind(this))
}

Bus.prototype.listen= function(name, handler) {
    this._events.on(name, function (evt) {

        var callback= function (err, message) {
            if (err) {
                if (err instanceof Error) {
                    err= { message:err.message }
                }
                this._setMessage(this.errorKeyName, evt.id, err, function () {
                    var key= [ this.errorChannelName, name ].join('::')
                    this.pub.publish(key, evt.id)
                }.bind(this))
                return
            }
            this._setMessage(this.successKeyName, evt.id, message, function () {
                var key= [ this.successChannelName, name ].join('::')
                this.pub.publish(key, evt.id)
            }.bind(this))
        }.bind(this)

        handler(evt.message, callback)

    }.bind(this))
}
