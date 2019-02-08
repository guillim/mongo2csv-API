let nodemailer = require('nodemailer');
const email    = require('../config/email');

// Create the transporter with the required configuration for Gmail
// change the user and pass !
let transporter = nodemailer.createTransport({
    host: email.smtp, // it could be smtp.zoho.com as well, or any SMTP provider
    port: 465,
    secure: true, // use SSL
    auth: {
        user: email.username,
        pass: email.password
    }
});

// setup e-mail data, even with unicode symbols
let mailOptions = function(to,link) {
		if(!to) throw err
		return {
			from: '"Alfred, the dontgomanual groom" <contact@dontgomanual.com>', // sender address (who sends)
			to: to, // list of receivers (who receives)
			subject: 'CSV available on WeTransfer | dontgomanual', // Subject line
			text: 'Please download your file at: '+link, // plaintext body
			html: 'Please download your file at: <br><b><a href="'+ link +'">Link to WeTransfer</a></b><br><br> You receive this message as the result of your request on dontgomanual platefrom' // html body
		}
};

// send mail with defined transport object
const sendEmail = function(obj) {
	if(!obj.to || !obj.link) throw 'sendEmail needs ->to and ->link'
	let mail = mailOptions(obj.to,obj.link)
	console.log('mail for:',obj.to,' - link is:',obj.link);

	transporter.sendMail(mail, function(error, info){
	    if(error){
					console.log(error);
	        return false
	    }
	    console.log('Message sent: ' + info.response);
			return true
	});
};

module.exports.sendEmail = sendEmail
