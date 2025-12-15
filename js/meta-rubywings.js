/* ============================================================
   RUBY WINGS – META PIXEL SYSTEM (FINAL – STABLE – SIMPLE)
   Version: 3.1
   ============================================================ */

(function () {
  'use strict';

  /* ============================================================
     1. CẤU HÌNH CỐ ĐỊNH
     ============================================================ */
  const CONFIG = {
    PIXEL_ID: '862531473384426',
    ENGAGED_TIME: 30000, // 30s
    SCROLL_DEPTH: 80
  };

  /* ============================================================
     2. CONTENT ID MAP – ĐẦY ĐỦ
     ============================================================ */
  const CONTENT_ID_MAP = {
    '': 'RW_HOME',

    // Landing
    'nhung-hanh-trinh-tieu-bieu': 'RW_LANDING_FEATURED',
    'nhung-hanh-trinh-tham-quan-trai-nghiem-linh-hoat': 'RW_LANDING_FLEX',

    // Tour tiêu biểu
    'ngon-lua-truong-son-2n1d': 'RW_TOUR_NLTSON_2N1D',
    'mua-do-va-tay-truong-son-2n1d': 'RW_TOUR_MUADO_TSON_2N1D',
    'hanh-trinh-lanh-dao-3n2d': 'RW_TOUR_LANHDAO_3N2D',
    'di-san-quoc-gia-2n1d': 'RW_TOUR_DISANQG_2N1D',
    'huyen-thoai-nguoi-phu-nu-viet-nam-3n2d': 'RW_TOUR_PHUNUVN_3N2D',
    'dmz-hoa-giai-ban-sac-4n3d': 'RW_TOUR_DMZ_4N3D',

    // Tour linh hoạt
    'hanh-trinh-trai-nghiem-linh-hoat': 'RW_FLEX_GENERAL',
    'hanh-trinh-giao-duc-ky-nang-song': 'RW_FLEX_EDU_SKILL',
    'hanh-trinh-trai-nghiem-thien-nhien': 'RW_FLEX_NATURE',
    'hanh-trinh-van-hoa-di-san': 'RW_FLEX_HERITAGE',

    // Hồ sơ – nhà trường
    'tom_tat_ho_so_hanh_trinh_mau_cho_nha_truong': 'RW_PROFILE_SCHOOL',

    // Giới thiệu
    '01-mo-dau-ruby-wings-la-ai': 'RW_ABOUT_01',
    '02-cau-chuyen-khoi-nguon': 'RW_ABOUT_02',
    '03-triet-ly-gia-tri': 'RW_ABOUT_03',
    '04-logo-y-nghia': 'RW_ABOUT_04',
    '05-he-sinh-thai-ruby-wings': 'RW_ABOUT_05',

    // Dịch vụ
    'dich-vu-du-lich-trai-nghiem': 'RW_SERVICE_EXPERIENCE',
    'dich-vu-retreat-chua-lanh': 'RW_SERVICE_RETREAT',
    'dich-vu-team-building': 'RW_SERVICE_TEAMBUILDING',
    'dich-vu-giao-duc-ky-nang': 'RW_SERVICE_EDUCATION',

    // Khác
    'lien-he': 'RW_CONTACT',
    'gioi-thieu': 'RW_ABOUT'
  };

  /* ============================================================
     3. TỪ KHOÁ LEAD – ĐẦY ĐỦ, KHÔNG THIẾU
     ============================================================ */
  const LEAD_KEYWORDS = [
    // VN
    'liên hệ', 'tư vấn', 'nhận tư vấn', 'đăng ký', 'đặt tour',
    'báo giá', 'yêu cầu', 'đặt lịch', 'gửi thông tin',
    'đăng ký ngay', 'liên hệ ngay', 'nhận báo giá',

    // EN
    'contact', 'consult', 'consultation', 'register',
    'book', 'booking', 'get quote', 'request',
    'inquiry', 'sign up', 'apply'
  ];

  /* ============================================================
     4. HÀM HỖ TRỢ
     ============================================================ */
  function getPathKey() {
    const path = window.location.pathname;
    const parts = path.split('/').filter(Boolean);
    return (parts.pop() || '').replace('.html', '');
  }

  function loadMetaPixel(callback) {
    if (window.fbq) {
      callback();
      return;
    }

    !function (f, b, e, v, n, t, s) {
      if (f.fbq) return;
      n = f.fbq = function () {
        n.callMethod ? n.callMethod.apply(n, arguments) : n.queue.push(arguments);
      };
      if (!f._fbq) f._fbq = n;
      n.push = n;
      n.loaded = true;
      n.version = '2.0';
      n.queue = [];
      t = b.createElement(e);
      t.async = true;
      t.src = v;
      s = b.getElementsByTagName(e)[0];
      s.parentNode.insertBefore(t, s);
      t.onload = callback;
    }(window, document, 'script', 'https://connect.facebook.net/en_US/fbevents.js');
  }

  /* ============================================================
     5. KHỞI TẠO PIXEL – CHUẨN META
     ============================================================ */
  function initPixel() {
    const pathKey = getPathKey();
    const contentId = CONTENT_ID_MAP[pathKey] || 'RW_UNKNOWN';

    fbq('init', CONFIG.PIXEL_ID);
    fbq('track', 'PageView');

    // ViewContent cho mọi trang trừ home
    if (pathKey !== '') {
      fbq('track', 'ViewContent', {
        content_ids: [contentId],
        content_name: document.title,
        content_type: 'product'
      });
    }

    // Scroll
    let scrollSent = false;
    window.addEventListener('scroll', function () {
      if (scrollSent) return;
      const percent = (window.scrollY + window.innerHeight) / document.documentElement.scrollHeight * 100;
      if (percent >= CONFIG.SCROLL_DEPTH) {
        scrollSent = true;
        fbq('trackCustom', 'DeepScroll', {
          content_id: contentId,
          scroll_depth: CONFIG.SCROLL_DEPTH
        });
      }
    });

    // Engagement
    setTimeout(function () {
      fbq('trackCustom', 'EngagedView', {
        content_id: contentId,
        time_on_page: CONFIG.ENGAGED_TIME / 1000
      });
    }, CONFIG.ENGAGED_TIME);

    // Click Lead
    document.addEventListener('click', function (e) {
      const el = e.target.closest('a,button');
      if (!el) return;

      const text = (el.innerText || '').toLowerCase();
      const href = (el.getAttribute('href') || '').toLowerCase();

      const isLead = LEAD_KEYWORDS.some(k => text.includes(k) || href.includes(k));
      if (isLead) {
        fbq('track', 'Lead', {
          content_id: contentId,
          button_text: text.substring(0, 100)
        });
      }
    }, true);
  }

  /* ============================================================
     6. CHẠY HỆ THỐNG
     ============================================================ */
  loadMetaPixel(initPixel);

})();
