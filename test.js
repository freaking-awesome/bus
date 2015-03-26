describe('Bus example', function () { it('readme', function (done) {



    //
    // Подключение...
    //

    var Bus= require('./index')



    //
    // Использование...
    //

    var b1= new Bus()
    var b2= new Bus()



    // регистрация обработчика

    b1.listen('something', function (message, callback) {
        callback(null, { data:true })
    })



    // обработка

    b2.handle('something', { data:false }, function (err, result) {
        console.assert(result.data === true)
        done()
    })



})})
