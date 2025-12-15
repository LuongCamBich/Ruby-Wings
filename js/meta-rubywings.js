/**
 * ============================================================
 * RUBY WINGS – META PIXEL SYSTEM (STABLE – SIMPLE – SAFE)
 * ============================================================
 * ✔ Chạy tốt với HTML tĩnh
 * ✔ Không cần CAPI
 * ✔ Không phụ thuộc code cũ
 * ✔ Tự khởi tạo fbq nếu chưa có
 * ✔ Không lỗi fbq is not defined
 * ✔ Không xung đột GA / Google Ads
 * ✔ Dễ bảo trì – dễ bàn giao
 * ============================================================
 */

(function () {
  'use strict';

  /* ============================================================
   * 1. CẤU HÌNH CHÍNH
   * ============================================================ */
  const CONFIG = {
    PIXEL_ID: '862531473384426',
    SCROLL_DEPTH: 80,        // %
    ENGAGED_TIME: 30000      // ms
  };

  /* ============================================================
   * 2. CONTENT ID MAP – ĐẦY ĐỦ TOÀN SITE
   * ============================================================ */
  const CONTENT_ID_MAP = {
    '': 'RW_HOME',

    // Landing chính
    'nhung-hanh-trinh-tieu-bieu': 'RW_LANDING_FEATURED',
    'nhung-hanh-trinh-tham-quan-trai-nghiem-linh-hoat': 'RW_LANDING_FLEX',

    // Tour tiêu biểu
    'ngon-lua-truong-son-2n1d': 'RW_TOUR_NLTSON_2N1D',
    'mua-do-va-tay-truong-son-2n1d': 'RW_TOUR_MUADO_2N1D',
    'hanh-trinh-lanh-dao-3n2d': 'RW_TOUR_LANHDAO_3N2D',
    'di-san-quoc-gia-2n1d': 'RW_TOUR_DISANQG_2N1D',
    'huyen-thoai-nguoi-phu-nu-viet-nam-3n2d': 'RW_TOUR_PHUNU_3N2D',
    'dmz-hoa-giai-ban-sac-4n3d': 'RW_TOUR_DMZ_4N3D',

    // Linh hoạt
    'hanh-trinh-trai-nghiem-linh-hoat': 'RW_FLEX_GENERAL',
    'hanh-trinh-giao-duc-ky-nang-song': 'RW_FLEX_EDU_SKILL',
    'hanh-trinh-trai-nghiem-thien-nhien': 'RW_FLEX_NATURE',
    'hanh-trinh-van-hoa-di-san': 'RW_FLEX_HERITAGE',

    // Hồ sơ – nhà trường
    'tom_tat_ho_so_hanh_trinh_mau_cho_nha_truong': 'RW_PROFILE_SCHOOL',

    // Câu chuyện
    '01-mo-dau-ruby-wings-la-ai': 'RW_STORY_ABOUT',
    '02-cau-chuyen-khoi-nguon': 'RW_STORY_ORIGIN',
    '03-triet-ly-gia-tri': 'RW_STORY_PHILOSOPHY',
    '04-logo-y-nghia': 'RW_STORY_LOGO',
    '05-he-sinh-thai-ruby-wings': 'RW_STORY_ECOSYSTEM',

    // Dịch vụ
    'dich-vu-du-lich-trai-nghiem': 'RW_SERVICE_EXPERIENCE',
    'dich-vu-retreat-chua-lanh': 'RW_SERVICE_RETREAT',
    'dich-vu-team-building': 'RW_SERVICE_TEAMBUILDING',
    'dich-vu-giao-duc-ky-nang': 'RW_SERVICE_EDUCATION',

    // Trang cơ bản
    'lien-he': 'RW_CONTACT',
    'gioi-thieu': 'RW_ABOUT'
  };

  /* ============================================================
   * 3. TỪ KHOÁ PHÁT HIỆN LEAD
   * ============================================================ */
  const LEAD_KEYWORDS = [
  /* ===============================
   * 1. LIÊN HỆ – TƯ VẤN CƠ BẢN
   * =============================== */
  'liên hệ', 'contact', 'liên lạc',
  'tư vấn', 'consult', 'consultation',
  'trao đổi', 'hỏi thêm', 'hỏi thông tin',
  'nhắn tin', 'gửi tin nhắn', 'inbox',
  'chat', 'message', 'messenger',

  /* ===============================
   * 2. ĐĂNG KÝ – THAM GIA – QUAN TÂM
   * =============================== */
  'đăng ký', 'register', 'registration',
  'tham gia', 'join', 'sign up',
  'ghi danh', 'đăng ký ngay',
  'quan tâm', 'tìm hiểu', 'tìm hiểu thêm',
  'xem chi tiết', 'xem chương trình',

  /* ===============================
   * 3. BÁO GIÁ – ĐỀ XUẤT – B2B
   * =============================== */
  'báo giá', 'quote', 'quotation',
  'yêu cầu báo giá', 'nhận báo giá',
  'xin báo giá', 'request quote',
  'đề xuất', 'proposal',
  'nhận đề xuất', 'xin đề xuất',
  'kế hoạch', 'phương án',
  'xây dựng chương trình',
  'thiết kế chương trình',
  'chương trình riêng',
  'chương trình theo yêu cầu',

  /* ===============================
   * 4. TOUR – BOOKING – LỊCH TRÌNH
   * =============================== */
  'đặt tour', 'booking', 'book tour',
  'đặt lịch', 'schedule', 'appointment',
  'lịch trình', 'itinerary',
  'giữ chỗ', 'reserve',
  'xác nhận tour', 'confirm',

  /* ===============================
   * 5. NHÀ TRƯỜNG – TỔ CHỨC (RẤT QUAN TRỌNG)
   * =============================== */
  'nhà trường', 'trường học',
  'ban giám hiệu', 'giáo viên',
  'phòng đào tạo', 'phòng công tác',
  'đoàn trường', 'liên đội',
  'học sinh', 'sinh viên',
  'tập thể', 'tổ chức',
  'đơn vị', 'institution',
  'educational', 'school',

  /* ===============================
   * 6. DOANH NGHIỆP – TEAM – RETREAT
   * =============================== */
  'doanh nghiệp', 'company', 'corporate',
  'team building', 'team-building',
  'retreat', 'workshop',
  'đào tạo', 'training',
  'huấn luyện', 'leadership',
  'phát triển đội ngũ',

  /* ===============================
   * 7. FORM – HÀNH ĐỘNG CUỐI PHỄU
   * =============================== */
  'điền form', 'form đăng ký',
  'gửi form', 'submit',
  'nhận hồ sơ', 'tải hồ sơ',
  'download', 'tải về',
  'nhận tài liệu', 'profile',
  'hồ sơ chương trình',

  /* ===============================
   * 8. HÀNH ĐỘNG GẤP – HIGH INTENT
   * =============================== */
  'ngay', 'bây giờ', 'liên hệ ngay',
  'đăng ký ngay', 'nhận ngay',
  'tư vấn ngay', 'gọi ngay',
  'call now', 'hotline',

  /* ===============================
   * 9. ĐIỆN THOẠI – EMAIL (IMPLICIT LEAD)
   * =============================== */
  'gọi', 'call', 'hotline',
  'điện thoại', 'phone',
  'email', 'mail', 'send mail',

  /* ===============================
   * 10. TIẾNG ANH – META HỌC CHÉO
   * =============================== */
  'inquiry', 'enquire',
  'request', 'information',
  'learn more', 'get info',
  'get consultation',
  'get quote'
];

  /* ============================================================
   * 4. KHỞI TẠO fbq AN TOÀN (CHẶN LỖI CODE CŨ)
   * ============================================================ */
  if (!window.fbq) {
    window.fbq = function () {
      window.fbq.queue.push(arguments);
    };
    window.fbq.queue = [];
    window.fbq.loaded = true;
    window.fbq.version = '2.0';
  }

  /* ============================================================
   * 5. LOAD META PIXEL SCRIPT (DUY NHẤT)
   * ============================================================ */
  if (!document.getElementById('rubywings-meta-pixel')) {
    const s = document.createElement('script');
    s.id = 'rubywings-meta-pixel';
    s.async = true;
    s.src = 'https://connect.facebook.net/en_US/fbevents.js';
    s.onload = function () {
      fbq('init', CONFIG.PIXEL_ID);
      fbq('track', 'PageView');
      console.log('✅ RubyWings Meta Pixel loaded');
    };
    document.head.appendChild(s);
  }

  /* ============================================================
   * 6. PHÂN TÍCH TRANG
   * ============================================================ */
  function getPathKey() {
    const path = location.pathname.split('/').pop() || '';
    return path.replace('.html', '');
  }

  const PATH_KEY = getPathKey();
  const CONTENT_ID = CONTENT_ID_MAP[PATH_KEY] || 'RW_UNKNOWN';

  /* ============================================================
   * 7. VIEW CONTENT
   * ============================================================ */
  setTimeout(function () {
    if (CONTENT_ID !== 'RW_HOME' && CONTENT_ID !== 'RW_UNKNOWN') {
      fbq('track', 'ViewContent', {
        content_ids: [CONTENT_ID],
        content_name: document.title,
        content_type: 'product'
      });
    }
  }, 1000);

  /* ============================================================
   * 8. SCROLL TRACKING
   * ============================================================ */
  let scrollTracked = false;
  window.addEventListener('scroll', function () {
    const percent =
      (window.scrollY + window.innerHeight) /
      document.documentElement.scrollHeight * 100;

    if (!scrollTracked && percent >= CONFIG.SCROLL_DEPTH) {
      scrollTracked = true;
      fbq('trackCustom', 'DeepScroll', {
        content_id: CONTENT_ID,
        scroll_depth: CONFIG.SCROLL_DEPTH
      });
    }
  }, { passive: true });

  /* ============================================================
   * 9. ENGAGED VIEW
   * ============================================================ */
  setTimeout(function () {
    fbq('trackCustom', 'EngagedView', {
      content_id: CONTENT_ID,
      time_on_page: CONFIG.ENGAGED_TIME / 1000
    });
  }, CONFIG.ENGAGED_TIME);

  /* ============================================================
   * 10. LEAD CLICK TRACKING
   * ============================================================ */
  document.addEventListener('click', function (e) {
    const el = e.target.closest('a,button');
    if (!el) return;

    const text = (el.textContent || '').toLowerCase();
    const href = (el.getAttribute('href') || '').toLowerCase();

    const isLead = LEAD_KEYWORDS.some(k =>
      text.includes(k) || href.includes(k)
    );

    if (isLead) {
      fbq('track', 'Lead', {
        content_id: CONTENT_ID,
        button_text: text.substring(0, 100)
      });
    }
  }, true);

})();
