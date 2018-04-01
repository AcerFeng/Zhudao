$(function() {
    $('#hidde-btn').click(function(){
        var icon = $('#hidde-icon');
        if (icon.hasClass('icon-left')) {
            icon.removeClass('icon-left').addClass('icon-right1'); 
            hiddenSide();
            longMain();
        } else if (icon.hasClass('icon-right1')) {
            icon.removeClass('icon-right1').addClass('icon-left'); 
            displaySide();
            shortMain();
        }


    });

    function hiddenSide() {
        $('#left-side').animate({
            left: '-240px'
        });
    }

    function displaySide() {
        $('#left-side').animate({
            left: '0'
        });
    }

    function longMain() {
        $('#main').animate({
            'margin-left': '50px'
        });
    }

    function shortMain() {
        $('#main').animate({
            'margin-left': '270px'
        });
    }
    

    
});


