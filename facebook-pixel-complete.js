// ==================================================
// FACEBOOK PIXEL + CAPI GATEWAY - RUBYWINGS.VN - FIXED
// ==================================================

// Load Facebook Pixel Base Code
(function() {
    if (window.fbqLoaded) return;
    window.fbqLoaded = true;
    
    !function(f,b,e,v,n,t,s)
    {if(f.fbq)return;n=f.bq=function(){n.callMethod?
    n.callMethod.apply(n,arguments):n.queue.push(arguments)};
    if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';
    n.queue=[];t=b.createElement(e);t.async=!0;
    t.src=v;s=b.getElementsByTagName(e)[0];
    s.parentNode.insertBefore(t,s)}(window, document,'script',
    'https://connect.facebook.net/en_US/fbevents.js');
    
    // ĐỢI PIXEL LOAD RỒI MỚI INIT
    fbq('loaded', function() {
        // Initialize Pixel với CAPI Gateway
        fbq('init', '862531473384426', {
            external_id: getVisitorID(),
            tn: 'CAPI Gateway'
        });
        
        // QUAN TRỌNG: ĐỢI 500ms RỒI MỚI SET BRIDGE
        setTimeout(function() {
            fbq('set', 'bridge', 'https://www.rubywings.vn/capi');
            console.log('Facebook Pixel bridge set successfully');
        }, 500);
        
        fbq('track', 'PageView');
        console.log('Facebook Pixel initialized successfully');
    });
    
    // FALLBACK: Nếu loaded event không fire
    setTimeout(function() {
        if (typeof fbq !== 'undefined' && fbq.loaded) {
            fbq('init', '862531473384426');
            fbq('track', 'PageView');
            console.log('Facebook Pixel fallback initialization');
        }
    }, 2000);
})();

// Hàm lấy Visitor ID cho CAPI
function getVisitorID() {
    let visitorId = localStorage.getItem('rw_visitor_id');
    if (!visitorId) {
        visitorId = 'rw_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
        localStorage.setItem('rw_visitor_id', visitorId);
    }
    return visitorId;
}

// CAPI Enhanced Tracking Functions
function trackConsultationClick() {
    // ĐẢM BẢO PIXEL ĐÃ SẴN SÀNG
    if (typeof fbq === 'undefined') {
        setTimeout(trackConsultationClick, 100);
        return;
    }
    
    const visitorData = {
        external_id: getVisitorID(),
        client_ip_address: '',
        client_user_agent: navigator.userAgent,
        event_source_url: window.location.href,
        action_source: 'website'
    };
    
    fbq('track', 'Lead', {
        content_name: 'Đăng ký tư vấn chung - ' + document.title,
        content_category: 'General Consultation',
        page_url: window.location.href,
        button_type: 'General CTA'
    }, visitorData);
    
    // Google Analytics tracking
    if (typeof gtag !== 'undefined') {
        gtag('event', 'consultation_click', {
            'event_category': 'engagement',
            'event_label': 'Google Forms Consultation Button'
        });
    }
    
    console.log('Facebook Pixel: Tracked consultation click');
}

// ... (giữ nguyên các hàm trackTourRegistration, trackTourView, etc.)

// Noscript fallback
document.write('<noscript><img height="1" width="1" style="display:none" src="https://www.facebook.com/tr?id=862531473384426&ev=PageView&noscript=1"/></noscript>');