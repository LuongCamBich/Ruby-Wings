<!-- ================================================== -->
<!-- FACEBOOK PIXEL COMPLETE - RUBY WINGS TRAVEL (FIXED) -->
<!-- ================================================== -->

<script>
// ==================================================
// FACEBOOK PIXEL BASE CODE
// ==================================================
!function(f,b,e,v,n,t,s)
{if(f.fbq)return;n=f.fbq=function(){n.callMethod?
n.callMethod.apply(n,arguments):n.queue.push(arguments)};
if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';
n.queue=[];t=b.createElement(e);t.async=!0;
t.src=v;s=b.getElementsByTagName(e)[0];
s.parentNode.insertBefore(t,s)}(window, document,'script',
'https://connect.facebook.net/en_US/fbevents.js');

// ==================================================
// INIT PIXEL - Advanced Matching + Custom ID
// ==================================================
fbq('init', '862531473384426', {
    external_id: getVisitorID(),
    em: getHashedEmail(),
    ph: getHashedPhone(),
    tn: 'Ruby Wings Travel'
});

// Optional: bridge CAPI (sau khi init)
fbq('set', 'bridge', 'https://www.rubywings.vn/capi');

// ==================================================
// BASIC PAGE TRACKING
// ==================================================
fbq('track', 'PageView', {
    content_name: document.title,
    content_category: 'Page View',
    page_url: window.location.href
});

console.log('[Ruby Wings] Facebook Pixel initialized successfully');

// ==================================================
// UTILITY FUNCTIONS
// ==================================================
function getVisitorID() {
    let visitorId = localStorage.getItem('rw_visitor_id');
    if (!visitorId) {
        visitorId = 'rw_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
        localStorage.setItem('rw_visitor_id', visitorId);
    }
    return visitorId;
}

function getHashedEmail() {
    // TODO: replace with SHA256 hash in production
    return '';
}

function getHashedPhone() {
    // TODO: replace with SHA256 hash in production
    return '';
}

// ==================================================
// TRACKING FUNCTIONS
// ==================================================
function trackConsultationSubmit(formData) {
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

function trackTourRegistration(tourName, tourCategory, price) {
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

function trackTourView(tourName, tourCategory) {
    fbq('track', 'ViewContent', {
        content_name: tourName,
        content_category: tourCategory,
        content_type: 'product',
        content_ids: [tourName],
        page_url: window.location.href
    });
    console.log('[Ruby Wings] Tracked tour view:', tourName);
}

function trackContactClick(contactMethod) {
    fbq('track', 'Contact', {
        content_name: 'Contact - ' + contactMethod,
        content_category: 'Contact Method',
        contact_method: contactMethod,
        page_url: window.location.href
    });
    console.log('[Ruby Wings] Tracked contact click:', contactMethod);
}

function trackDonation(amount, cause) {
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

// ==================================================
// AUTO TRACKING
// ==================================================
document.addEventListener('DOMContentLoaded', function() {
    setTimeout(function() {
        const url = window.location.href;
        const title = document.title;

        if (url.includes('du-lich-trai-nghiem-cam-xuc') || title.includes('Trải nghiệm cảm xúc')) {
            trackTourView('Du lịch Trải nghiệm Cảm xúc', 'Experience Tourism');
        } else if (url.includes('du-lich-chua-lanh') || title.includes('Chữa lành')) {
            trackTourView('Du lịch Chữa lành', 'Healing Tourism');
        } else if (url.includes('retreat-thien') || title.includes('Retreat')) {
            trackTourView('Retreat Thiền và Khí công', 'Wellness Retreat');
        } else if (url.includes('team-building') || title.includes('Team Building')) {
            trackTourView('Team Building Trải nghiệm', 'Corporate Events');
        } else if (url.includes('qr-menu') || title.includes('QR Menu')) {
            trackTourView('QR Menu Generator', 'Utility Tool');
        }

        console.log('[Ruby Wings] Auto page tracking completed');
    }, 2000);
});

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

// ==================================================
// ERROR & LOAD HANDLING
// ==================================================
function ensurePixelLoaded() {
    if (typeof fbq === 'undefined' || !window.fbq.getState) {
        console.log('[Ruby Wings] Waiting for Facebook Pixel to load...');
        setTimeout(ensurePixelLoaded, 500);
    } else {
        console.log('[Ruby Wings] Facebook Pixel ready:', fbq.getState());
    }
}
ensurePixelLoaded();

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

<noscript>
    <img height="1" width="1" style="display:none"
         src="https://www.facebook.com/tr?id=862531473384426&ev=PageView&noscript=1"
         alt="Facebook Pixel NoScript" />
</noscript>
