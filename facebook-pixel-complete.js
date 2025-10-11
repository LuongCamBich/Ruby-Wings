// ==================================================
// FACEBOOK PIXEL - RUBYWINGS.VN - SIMPLE & STABLE
// ==================================================

// Load Facebook Pixel Base Code
(function() {
    if (window.fbqLoaded) return;
    window.fbqLoaded = true;
    
    !function(f,b,e,v,n,t,s)
    {if(f.fbq)return;n=f.fbq=function(){n.callMethod?
    n.callMethod.apply(n,arguments):n.queue.push(arguments)};
    if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';
    n.queue=[];t=b.createElement(e);t.async=!0;
    t.src=v;s=b.getElementsByTagName(e)[0];
    s.parentNode.insertBefore(t,s)}(window, document,'script',
    'https://connect.facebook.net/en_US/fbevents.js');
    
    // INIT ĐƠN GIẢN - KHÔNG BRIDGE, KHÔNG CALLBACK
    setTimeout(function() {
        if (typeof fbq !== 'undefined') {
            fbq('init', '862531473384426', {
                external_id: getVisitorID()
            });
            fbq('track', 'PageView');
            console.log('Facebook Pixel initialized successfully - No Bridge');
        }
    }, 1000);
})();

// Hàm lấy Visitor ID
function getVisitorID() {
    let visitorId = localStorage.getItem('rw_visitor_id');
    if (!visitorId) {
        visitorId = 'rw_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
        localStorage.setItem('rw_visitor_id', visitorId);
    }
    return visitorId;
}

// Tracking Functions - SIMPLIFIED
function trackConsultationClick() {
    if (typeof fbq === 'undefined') {
        console.log('Pixel not ready, retrying...');
        setTimeout(trackConsultationClick, 200);
        return;
    }
    
    fbq('track', 'Lead', {
        content_name: 'Đăng ký tư vấn chung - ' + document.title,
        content_category: 'General Consultation', 
        page_url: window.location.href,
        button_type: 'General CTA'
    });
    
    if (typeof gtag !== 'undefined') {
        gtag('event', 'consultation_click', {
            'event_category': 'engagement',
            'event_label': 'Google Forms Consultation Button'
        });
    }
    
    console.log('Facebook Pixel: Tracked consultation click');
}

// Noscript fallback
document.write('<noscript><img height="1" width="1" style="display:none" src="https://www.facebook.com/tr?id=862531473384426&ev=PageView&noscript=1"/></noscript>');