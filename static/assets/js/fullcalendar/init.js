$(document).ready(function() {

    var date = new Date();
    var d = date.getDate();
    var m = date.getMonth();
    var y = date.getFullYear();

    // page is now ready, initialize the calendar...

    var calendar = $('#calendar');

    calendar.fullCalendar({
        events: [
            {
                title: 'All Day Event',
                start: new Date(y, m, 1, 8, 45)
            },
            {
                title: 'Long Event',
                start: new Date(y, m, d-5, 12, 15),
                end: new Date(y, m, d-2)
            },
            {
                id: 999,
                title: 'Repeating Event',
                start: new Date(y, m, d-3, 16, 30),
                allDay: false
            },
            {
                title: 'Meeting',
                start: '2016-07-12T10:30:00',
                end: '2016-07-12T12:30:00'
            },
            {
                title: 'Lunch',
                start: '2016-07-12T12:00:00'
            },
            {
                title: 'Click for Google',
                url: 'http://google.com/',
                start: '2016-07-28'
            }
        ]
    });


    calendar.fullCalendar( 'addEventSource',
        function(start, end, status, callback) {
            // When requested, dynamically generate virtual
            // events for every monday and wednesday.
            var events = [];

            console.log(start._d.getTime())
            console.log(end._d.getTime())

            for (var loop = start._d.getTime();
                 loop <= end._d.getTime();
                 loop = loop + (24 * 60 * 60 * 1000)) {

                var test_date = new Date(loop);
                test_date.setHours(6, 30)

                if (test_date.getDay() === 1) {
                    // we're in Moday, create the event
                    events.push({
                        title: 'WOW it\'s monday :)',
                        start: test_date
                    });
                }

            } // for loop

            // return events generated
            callback( events );
        }
    );

});