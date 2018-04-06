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
            if ($(window).width()>769) {
                $('#main').removeClass('main-move-left');
                $('#main').addClass('main-move-right');
            }
        }
    });

    resetItemWidth();
});


function isHiddenSide() {
    return $('#left-side').css('left') !== '0px';
}

$(window).resize(function() {
    resetItemWidth();
});

function resetItemWidth() {
    var screenWidth = $(window).width();
    var mainWidth = screenWidth - (isHiddenSide() ? 0 : 240);
    var itemWidth = '';
    var margin = 20 + 30;
    var num = 10;
    if (screenWidth < 678) {
        num = 3;
        margin = 15 * 2;
    } else if (screenWidth < 900) {
        num = 5;
    } else if (screenWidth < 1100) {
        num = 6;
    } else if (screenWidth < 1300) {
        num = 7;
    } else {
        num = Math.floor(screenWidth / 200) + 1
    }

    itemWidth = Math.floor((mainWidth - margin - (num - 1) * 20 ) / num) + 'px';

    var itemStyle = itemWidth ? '.category-list .item { width: ' + itemWidth + '!important}' : '';

    $('#st').html('<style>' + itemStyle + '</style>')
}


