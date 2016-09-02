UserRatings = new Mongo.Collection('user_ratings');
Predictions = new Mongo.Collection('predictions');

if (Meteor.isClient) {
  Session.set('userId', 2);
  Template.body.helpers({
    user_ratings: function () {
		return UserRatings.find({});
    },
	predictions: function () {
		var userId = Session.get('userId')
		return Predictions.find({userId:userId},{sort: {timestamp: -1}});
	}
  });
  Template.body.events({
	'submit .new-user':function(event) {
		event.preventDefault();
		Session.set('userId',parseInt(event.target.user.value))
	}
  });
}


//predictions.find({userId:2},{sort: {timestamp: -1}})
//predictions.find().sort({'timestamp': -1})