!function ($) {

  $(function(){

    var $window = $(window)

    $('section [href^=#]').click(function (e) {
      e.preventDefault()
    })

    $('div.docs-sidebar > ul').affix({
      offset: {
        top: 210
      , bottom: 50
      }
    })

  })

}(window.jQuery)
