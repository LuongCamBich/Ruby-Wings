<!-- ================================================== -->
<!-- FACEBOOK PIXEL COMPLETE - RUBY WINGS TRAVEL -->
<!-- ================================================== -->

<!-- Facebook Pixel Code -->
<script>
// Prevent automatic bridge injection
window.fbq = window.fbq || function() {
    // Block bridge calls completely
    if (arguments[0] === 'set' && arguments[1] === 'bridge') {
        console.log('[Ruby Wings] Facebook Pixel Bridge blocked');
        return;
    }
    
    // Normal fbq functionality for other calls
    if (!window._fbq) window._fbq = [];
    window._fbq.push(arguments);
};

// Load Facebook Pixel Base Code
!function(f,b,e,v,n,t,s)
{if(f.fbq)return;n=f.fbq=function(){n.callMethod?
n.callMethod.apply(n,arguments):n.queue.push(arguments)};
if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';
n.queue=[];t=b.createElement(e);t.async=!0;
t.src=v;s=b.getElementsByTagName(e)[0];
s.parentNode.insertBefore(t,s)}(window, document,'script',
'https://connect.facebook.net/en_US/fbevents.js');

// Initialize Pixel with Advanced Matching
fbq('init', '862531473384426', {
    external_id: getVisitorID(),
    em: getHashedEmail(),
    ph: getHashedPhone(),
    tn: 'Ruby Wings Travel'
});

// Track Page View
fbq('track', 'PageView', {
    content_name: document.title,
    content_category: 'Page View',
    page_url: window.location.href
});

console.log('[Ruby Wings] Facebook Pixel initialized successfully');

// ==================== UTILITY FUNCTIONS ====================

// Generate Visitor ID
function getVisitorID() {
    let visitorId = localStorage.getItem('rw_visitor_id');
    if (!visitorId) {
        visitorId = 'rw_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
        localStorage.setItem('rw_visitor_id', visitorId);
    }
    return visitorId;
}

// Hash email for Advanced Matching (placeholder)
function getHashedEmail() {
    // In production, hash the email properly
    return '';
}

// Hash phone for Advanced Matching (placeholder)  
function getHashedPhone() {
    // In production, hash the phone properly
    return '';
}

// ==================== TRACKING FUNCTIONS ====================

// Track Consultation Form Submission
function trackConsultationSubmit(formData) {
    if (typeof fbq === 'undefined') {
        setTimeout(function() { trackConsultationSubmit(formData); }, 100);
        return;
    }
    
    fbq('track', 'Lead', {
        content_name: 'Consultation Form - ' + document.title,
        content_category: 'General Consultation',
        content_type: 'form',
        page_url: window.location.href,
        button_type: 'Consultation CTA',
        form_type: 'consultation',
        form_data: JSON.stringify(formData)
    });
    
    console.log('[Ruby Wings] Tracked consultation form submission');
}

// Track Tour Registration
function trackTourRegistration(tourName, tourCategory, price) {
    if (typeof fbq === 'undefined') {
        setTimeout(function() { trackTourRegistration(tourName, tourCategory, price); }, 100);
        return;
    }
    
    fbq('track', 'CompleteRegistration', {
        content_name: tourName,
        content_category: tourCategory,
        content_type: 'product',
        content_ids: [tourName],
        value: price,
        currency: 'VND',
        page_url: window.location.href,
        registration_method: 'online'
    });
    
    console.log('[Ruby Wings] Tracked tour registration:', tourName);
}

// Track Tour View
function trackTourView(tourName, tourCategory) {
    if (typeof fbq === 'undefined') {
        setTimeout(function() { trackTourView(tourName, tourCategory); }, 100);
        return;
    }
    
    fbq('track', 'ViewContent', {
        content_name: tourName,
        content_category: tourCategory,
        content_type: 'product',
        content_ids: [tourName],
        page_url: window.location.href
    });
    
    console.log('[Ruby Wings] Tracked tour view:', tourName);
}

// Track Contact Click
function trackContactClick(contactMethod) {
    if (typeof fbq === 'undefined') {
        setTimeout(function() { trackContactClick(contactMethod); }, 100);
        return;
    }
    
    fbq('track', 'Contact', {
        content_name: 'Contact - ' + contactMethod,
        content_category: 'Contact Method',
        contact_method: contactMethod,
        page_url: window.location.href
    });
    
    console.log('[Ruby Wings] Tracked contact click:', contactMethod);
}

// Track Donation
function trackDonation(amount, cause) {
    if (typeof fbq === 'undefined') {
        setTimeout(function() { trackDonation(amount, cause); }, 100);
        return;
    }
    
    fbq('track', 'Donate', {
        value: amount,
        currency: 'VND',
        content_name: 'Donation - ' + cause,
        content_category: 'Charity',
        donation_cause: cause,
        page_url: window.location.href
    });
    
    console.log('[Ruby Wings] Tracked donation:', amount, 'for', cause);
}

// ==================== AUTO TRACKING ====================

// Auto-detect and track page types
document.addEventListener('DOMContentLoaded', function() {
    setTimeout(function() {
        const url = window.location.href;
        const title = document.title;
        
        // Track specific page types
        if (url.includes('du-lich-trai-nghiem-cam-xuc') || title.includes('Trải nghiệm cảm xúc')) {
            trackTourView('Du lịch Trải nghiệm Cảm xúc', 'Experience Tourism');
        }
        else if (url.includes('du-lich-chua-lanh') || title.includes('Chữa lành')) {
            trackTourView('Du lịch Chữa lành', 'Healing Tourism');
        }
        else if (url.includes('retreat-thien') || title.includes('Retreat')) {
            trackTourView('Retreat Thiền và Khí công', 'Wellness Retreat');
        }
        else if (url.includes('team-building') || title.includes('Team Building')) {
            trackTourView('Team Building Trải nghiệm', 'Corporate Events');
        }
        else if (url.includes('qr-menu') || title.includes('QR Menu')) {
            trackTourView('QR Menu Generator', 'Utility Tool');
        }
        
        console.log('[Ruby Wings] Auto page tracking completed');
    }, 2000);
});

// Track consultation button clicks
document.addEventListener('DOMContentLoaded', function() {
    const consultationButtons = document.querySelectorAll('[onclick*="trackConsultation"], .consultation-btn, .rw-consult-btn-main');
    
    consultationButtons.forEach(function(button) {
        button.addEventListener('click', function() {
            setTimeout(function() {
                trackConsultationSubmit({
                    button_text: button.textContent.trim(),
                    button_type: 'consultation',
                    timestamp: new Date().toISOString()
                });
            }, 500);
        });
    });
});

// ==================== ERROR HANDLING ====================

// Ensure Pixel loads
function ensurePixelLoaded() {
    if (typeof fbq === 'undefined') {
        console.log('[Ruby Wings] Waiting for Facebook Pixel to load...');
        setTimeout(ensurePixelLoaded, 500);
    }
}
ensurePixelLoaded();

// Error tracking
window.addEventListener('error', function(e) {
    if (typeof fbq !== 'undefined') {
        fbq('track', 'PageView', {
            error_message: e.message,
            error_file: e.filename,
            error_line: e.lineno,
            error_type: 'javascript_error'
        });
    }
});

</script>

<!-- Noscript Fallback -->
<noscript>
    <img height="1" width="1" style="display:none" 
         src="https://www.facebook.com/tr?id=862531473384426&ev=PageView&noscript=1"
         alt="Facebook Pixel NoScript" />
</noscript>

<!-- End Facebook Pixel Code -->