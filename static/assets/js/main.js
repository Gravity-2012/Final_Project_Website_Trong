/**
 * createIT main javascript file.
 */
"use strict";

var $devicewidth = (window.innerWidth > 0) ? window.innerWidth : screen.width;
var $deviceheight = (window.innerHeight > 0) ? window.innerHeight : screen.height;
var $bodyel = jQuery("body");
var $navbarel = jQuery(".ct-menu");
var $topbarel = jQuery(".ct-topBar");

var $lgWidth = 1199;
var $mdWidth = 991;
var $smWidth = 767;
var $xsWidth = 479;
/* ========================== */
/* ==== HELPER FUNCTIONS ==== */

function validatedata($attr, $defaultValue) {
    "use strict";
    if ($attr !== undefined) {
        return $attr
    }
    return $defaultValue;
}

function parseBoolean(str, $defaultValue) {
    "use strict";
    if (str == 'true') {
        return true;
    } else if (str == "false") {
        return false;
    }
    return $defaultValue;
}

(function ($) {
    "use strict";
    if(document.getElementById('ct-js-wrapper')){
        var snapper = new Snap({
            element: document.getElementById('ct-js-wrapper')
        });

        snapper.settings({
            disable: "left",
            easing: 'ease',
            addBodyClasses: true
        });
    }

    $(document).ready(function () {

        if(typeof $.simpleWeather === "function"){
            $.simpleWeather({
                location: 'Warszawa',
                woeid: '',
                unit: 'c',
                success: function(weather) {
                    var html = '<h2><i class="icon-'+weather.code+'"></i> '+weather.temp+'&deg;'+weather.units.temp+'</h2>';
                    html += '<ul><li>'+weather.city+', '+weather.region+'</li>';
                    html += '<li class="currently">'+weather.currently+'</li>';

                    $("#ct-js-weather").html(html);
                },
                error: function(error) {
                    $("#ct-js-weather").html('<p>'+error+'</p>');
                }
            });
        }        



        // Add Color // -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        $(".ct-js-color").each(function(){
            $(this).css("color", '#' + $(this).attr("data-color"))
        });

        $('.ct-js-btnScrollUp').on("click",function (e) {
            e.preventDefault();
            $("body,html").animate({scrollTop: 0}, 1200);
        });

        // Snap Navigation in Mobile // -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        if ($devicewidth > 767 && document.getElementById('ct-js-wrapper')) {
            snapper.disable();
        }

        $(".navbar-toggle").click(function () {
            if($bodyel.hasClass('snapjs-right')){
                snapper.close();
            } else{
                snapper.open('right');
            }
        });

        $('.ct-menuMobile .ct-menuMobile-navbar .dropdown > a').click(function(e) {
            return false; // iOS SUCKS
        });
        $('.ct-menuMobile .ct-menuMobile-navbar .dropdown > a').click(function(e){
            var $this = $(this);
            if($this.parent().hasClass('open')){
                $(this).parent().removeClass('open');
            } else{
                $('.ct-menuMobile .ct-menuMobile-navbar .dropdown.open').toggleClass('open');
                $(this).parent().addClass('open');
            }
        });

        $('.ct-menuMobile .ct-menuMobile-navbar .onepage > a').click(function(e) {
            snapper.close();
        });

        // Animations Init // -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        if ($().appear) {
            if (device.mobile() || device.tablet()) {
                $("body").removeClass("cssAnimate");
            } else {

                $('.cssAnimate .animated').appear(function () {
                    var $this = $(this);

                    $this.each(function () {
                        if ($this.data('time') != undefined) {
                            setTimeout(function () {
                                $this.addClass('activate');
                                $this.addClass($this.data('fx'));
                            }, $this.data('time'));
                        } else {
                            $this.addClass('activate');
                            $this.addClass($this.data('fx'));
                        }
                    });
                }, {accX: 50, accY: -350});
            }
        }

        // Tooltips and Popovers // -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        $("[data-toggle='tooltip']").tooltip();

        $("[data-toggle='popover']").popover({trigger: "hover", html: true});

        // Link Scroll to Section // -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        //$('.ct-js-btnScroll[href^="#"]').click(function (e) {
        //    e.preventDefault();
        //
        //    var target = this.hash, $target = $(target);
        //
        //    $('html, body').stop().animate({
        //        'scrollTop': $target.offset().top + 0
        //    }, 900, 'swing', function () {
        //        window.location.hash = target;
        //    });
        //});
        //$('.ct-js-btnScrollUp').click(function (e) {
        //    e.preventDefault();
        //    $("body,html").animate({scrollTop: 0}, 500);
        //    $navbarel.find('.onepage').removeClass('active');
        //    $navbarel.find('.onepage:first-child').addClass('active');
        //    return false;
        //});

        // Navbar Search // -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        var $searchform = $(".ct-navbar-search");
        $('#ct-js-navSearch').click(function(e){
            e.preventDefault();

            $(this).toggleClass('is-active');
            $searchform.fadeToggle(250, function () {
                if (($searchform).is(":visible")) {
                    $searchform.find("[type=text]").focus();
                }
            });
            return false;
        });

        // Placeholder Fallback // -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        if ($().placeholder) {
            $("input[placeholder],textarea[placeholder]").placeholder();
        }

        //Data BG Inits
        if($(".ct-js-bg").length > 0){
            $(".ct-js-bg").each(function(){
                if($(this).attr("data-bg")) {
                    $(this).css('background', 'url("'+ $(this).attr("data-bg") +'")');
                }
                if($(this).attr("data-height")){
                    $(this).css('height', $(this).attr("data-height") + "px");
                }
            });
        }

        //MAGNIFIC POPUP
        if(jQuery().magnificPopup){
            jQuery('.ct-js-popupGallery').each(function() { // the containers for all your galleries
                jQuery(this).magnificPopup({
                    disableOn: 320,
                    type: 'image',
                    mainClass: 'ct-magnificPopup--image',
                    removalDelay: 160,
                    preloader: true,
                    delegate: '.ct-js-magnificPopupImage',
                    closeBtnInside: true,
                    closeOnContentClick: false,
                    closeOnBgClick: true,
                    gallery: {
                        enabled: true
                    },
                    callbacks: {
                        buildControls: function() {
                            // re-appends controls inside the main container
                            this.contentContainer.append(this.arrowLeft.add(this.arrowRight));
                        }
                    }
                });
            });
        }

        //SELECT
        $(".ct-select").select2();

        $(".ct-selectAdults").select2({
            placeholder: "No.Adults"
        });
        $(".ct-selectRooms").select2({
            placeholder: "No.Rooms"
        });
        $(".ct-selectChildren").select2({
            placeholder: "No.Children (3-12)"
        });
        $(".ct-selectInfants").select2({
            placeholder: "No.Infants (0-2)"
        });
        $(".ct-selectCurrency").select2({
            placeholder: "Currency"
        });
        $(".ct-selectBudget").select2({
            placeholder: "Budget"
        });
        $(".ct-selectMonth").select2({
            placeholder: "Month"
        });
        $(".ct-selectYear").select2({
            placeholder: "Year"
        });
        $(".ct-selectCountry").select2({
            placeholder: "Country"
        });

        $('.datepicker').datepicker({
            format: 'mm/dd/yyyy',
            startDate: '-3d'
        });

        function goToByScroll(id) {
            $('html,body').animate({scrollTop: $(id).offset().top - 70}, 1000);
        }
        $('body .ct-js-btnScroll').on("click",function () {
            goToByScroll($(this).attr('href'));
            return false;
        });


        $('.progress-icons').each(function () {
            var $this = $(this);
            var $total = $this.attr("data-total");
            var $icon = $this.attr("data-icon");
            var htmldata = "";

            $this.css("font-size", ($this.attr("data-font-size") + "px"));

            var i;
            for (i = 0; i < $total; i++) {
                htmldata += '<i class="fa ' + $icon + '"></i> ';
            }

            $this.html(htmldata);

            if (($().appear) && ($("body").hasClass("cssAnimate"))) {
                $('.progress-icons').appear(function () {
                    var $this = $(this);
                    var $active = $this.attr("data-active");
                    var $icons = $this.find('i:lt(' + $active + ')');
                    var $delay = parseInt(validatedata($this.attr("data-delay"), 20));

                    var delay = $delay;
                    for (i = 0; i < $icons.length; i++) {
                        setTimeout((function (i) {
                            return function () {
                                i.style.color = $this.attr("data-icon-color");
                            }
                        })($icons[i]), delay);
                        delay += $delay;
                    }
                }, {accY: -100});
            } else {
                $this.each(function () {
                    var $active = $this.attr("data-active");
                    var $icons = $this.find('i:lt(' + $active + ')');
                    $icons.css('color', $this.attr("data-icon-color"))
                });
            }
        });

        $('.ct-js-pieChart').each(function () {
            var $this = $(this);
            var $color = validatedata($(this).attr('data-ct-firstColor'), "#2b8be9");
            var $color2 = validatedata($(this).attr('data-ct-secondColor'), "#eeeeee");
            var $cutout = validatedata($(this).attr('data-ct-middleSpace'), 90);
            var $stroke = validatedata($(this).attr('data-ct-showStroke'), false);
            var $margin = validatedata($(this).attr('data-ct-margin'), false);
            $(this).parent().css('margin-left',$margin + 'px');
            $(this).parent().css('margin-right',$margin + 'px');
            var options = {
                responsive: true, percentageInnerCutout: $cutout, segmentShowStroke: $stroke, showTooltips: false
            }
            var doughnutData = [{
                value: parseInt($this.attr('data-ct-percentage')), color: $color, label: false
            }, {
                value: parseInt(100 - $this.attr('data-ct-percentage')), color: $color2
            }];

            if (($().appear) && ($("body").hasClass("cssAnimate"))) {
                $('.ct-js-pieChart').appear(function () {
                    var ctx = $this[0].getContext("2d");
                    window.myDoughnut = new Chart(ctx).Doughnut(doughnutData, options);
                });
            } else {
                var ctx = $this[0].getContext("2d");
                window.myDoughnut = new Chart(ctx).Doughnut(doughnutData, options);
            }
        });

        if (($().appear) && ($("body").hasClass("cssAnimate"))) {
            $('.progress').appear(function () {
                var $this = $(this);
                $this.each(function () {
                    var $innerbar = $this.find(".progress-bar");
                    var percentage = $innerbar.attr("aria-valuenow");
                    $innerbar.addClass("animating").css("width", percentage + "%");

                });
            }, {accY: -100});
        } else {
            $('.progress').each(function () {
                var $this = $(this);
                var $innerbar = $this.find(".progress-bar");
                var percentage = $innerbar.attr("aria-valuenow");
                $innerbar.addClass("animating").css("width", percentage + "%");

            });
        }

        $('.navbar-nav > li.labeled').each(function(){

            if($(this).attr('data-label')){
                $(this).append('<span>'+$(this).attr('data-label')+'</span>');
            }
        });

    });

    $(window).on('resize', function() {
        if ($(window).width() < 768) {
            snapper.enable();
        } else{
            snapper.disable();
        }
    });

    $(document).mouseup(function (e) {
        var $searchform = $(".ct-navbar-search");

        if(!$('#ct-js-navSearch').is(e.target)){
            if (!$searchform.is(e.target) // if the target of the click isn't the container...
                && $searchform.has(e.target).length === 0) // ... nor a descendant of the container
            {
                $searchform.hide();
                $('#ct-js-navSearch').removeClass('is-active');
            }
        }
    });

    $(window).load(function(){
        // Masonry For Sidebar // -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        if (jQuery().masonry  && (jQuery(window).width()<992) && (jQuery(window).width()>767)) {

            jQuery('.ct-js-sidebar .row').masonry({
                itemSelector: '.col-sm-6.col-md-12',
                layoutMode: 'sloppyMasonry',
                resizable: false, // disable normal resizing
                // set columnWidth to a percentage of container width
                masonry: { }
            });
        }

        var $preloader = $('.ct-preloader');
        var $content = $('.ct-preloader-content');

        var $timeout = setTimeout(function(){
            $($preloader).addClass('animated').addClass('fadeOut');
            $($content).addClass('animated').addClass('fadeOut');
        }, 0);
        var $timeout2 = setTimeout(function(){
            $($preloader).css('display', 'none').css('z-index', '-9999');
        }, 500);
    });

    $(window).scroll(function(){
        var scroll = $(window).scrollTop();

        if (scroll > 400) {
            jQuery('.ct-js-btnScrollUp').addClass('is-active');
        } else {
            jQuery('.ct-js-btnScrollUp').removeClass('is-active');
        }
    });



})(jQuery);