$(function() {
    $('#hidde-btn').click(function(){
        var icon = $('#hidde-icon');
        if ($('#left-side').css('left') === '0px') {
            icon.removeClass('icon-left').addClass('icon-right1');
            $('#left-side').removeClass('show-side');
            $('#left-side').addClass('hidden-side');
            $('#main').removeClass('main-move-right');
            $('#main').addClass('main-move-left');
        } else {
            icon.removeClass('icon-right1').addClass('icon-left');
            $('#left-side').removeClass('hidden-side');
            $('#left-side').addClass('show-side');
            $('#main').removeClass('main-move-left');
            $('#main').addClass('main-move-right');
        }


    });
    
});


