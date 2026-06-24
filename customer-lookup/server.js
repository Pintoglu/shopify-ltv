const express = require('express');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const AdmZip = require('adm-zip');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = 3000;

const UPLOAD_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`),
});

// Use upload.any() to avoid Multer v2's pre-filter slot decrement issue.
// Field name and file type validation is done in each route handler.
const upload = multer({ storage });

function groupFilesByField(files) {
  const map = {};
  (files || []).forEach(f => {
    if (!map[f.fieldname]) map[f.fieldname] = [];
    map[f.fieldname].push(f);
  });
  return map;
}


// Expand a list of uploaded file infos — unzip any .zip files and return flat
// array of { path, originalname } for all CSVs found.
function expandFiles(fileInfos) {
  const result = [];
  fileInfos.forEach(({ path: filePath, originalname }) => {
    if (originalname.endsWith('.zip')) {
      const zip = new AdmZip(filePath);
      zip.getEntries().forEach(entry => {
        if (entry.entryName.endsWith('.csv') && !entry.isDirectory) {
          const outPath = path.join(UPLOAD_DIR, `${Date.now()}-${path.basename(entry.entryName)}`);
          zip.extractEntryTo(entry, UPLOAD_DIR, false, true, false, path.basename(outPath));
          result.push({ path: outPath, originalname: path.basename(entry.entryName), _temp: true });
        }
      });
    } else {
      result.push({ path: filePath, originalname });
    }
  });
  return result;
}

app.use(express.static(path.join(__dirname, 'public')));

// ── CSV helpers ────────────────────────────────────────────────────────────────

function readCSV(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  // strip leading apostrophes Shopify adds to numeric fields
  const cleaned = raw.replace(/(?<=,|^)'(?=\d)/gm, '');
  try {
    return parse(cleaned, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      relax_quotes: true,
      relax_column_count: true,
    });
  } catch (e) {
    // fallback: try without relax_column_count
    return parse(cleaned, { columns: true, skip_empty_lines: true, trim: true });
  }
}

function normalizeEmail(e) {
  return (e || '').toLowerCase().trim();
}

function detectEmailCol(headers) {
  return (
    headers.find(h => /^email$/i.test(h)) ||
    headers.find(h => /customer.?email/i.test(h)) ||
    headers.find(h => /email/i.test(h) && !/accepts/i.test(h)) ||
    headers[0]
  );
}

function detectSalesCol(headers) {
  return (
    headers.find(h => /total.?sales/i.test(h)) ||
    headers.find(h => /sales/i.test(h)) ||
    headers.find(h => /revenue/i.test(h)) ||
    headers.find(h => /spend/i.test(h) && !/total/i.test(h)) ||
    null
  );
}

// ── Streaming helper ───────────────────────────────────────────────────────────

function makeStreamer(res) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('X-Accel-Buffering', 'no');
  const send = (data) => res.write(`data: ${JSON.stringify(data)}\n\n`);
  return {
    progress: (message) => send({ type: 'progress', message }),
    result:   (data)    => { send({ type: 'result', ...data }); res.end(); },
    error:    (message) => { send({ type: 'error', error: message }); res.end(); },
  };
}

// ── POST /api/lookup ───────────────────────────────────────────────────────────

app.post(
  '/api/lookup',
  upload.any(),
  (req, res) => {
    const uploadedPaths = [];
    const stream = makeStreamer(res);
    try {
      const filesByField = groupFilesByField(req.files);
      const emailFileInfo = filesByField['emailFile']?.[0];
      const exportFileInfos = filesByField['exportFiles'] || [];

      if (!emailFileInfo) return stream.error('No email list file uploaded.');
      if (!exportFileInfos.length) return stream.error('No customer export files uploaded.');

      stream.progress('Parsing email list…');
      const expandedExports = expandFiles(exportFileInfos);
      uploadedPaths.push(emailFileInfo.path, ...exportFileInfos.map(f => f.path), ...expandedExports.filter(f => f._temp).map(f => f.path));

      const emailRows = readCSV(emailFileInfo.path);
      if (!emailRows.length) return stream.error('Email list CSV is empty.');

      const emailHeaders = Object.keys(emailRows[0]);
      const emailCol = detectEmailCol(emailHeaders);
      const salesCol = detectSalesCol(emailHeaders.filter(h => h !== emailCol));

      const emailMap = new Map();
      emailRows.forEach(r => {
        const email = normalizeEmail(r[emailCol]);
        if (!email) return;
        const entry = { _sales: salesCol ? (r[salesCol] || '') : '' };
        emailHeaders.forEach(h => {
          if (h !== emailCol && h !== salesCol) entry[`_list_${h}`] = r[h] || '';
        });
        emailMap.set(email, entry);
      });

      stream.progress(`Parsed ${emailMap.size.toLocaleString()} emails — scanning export files…`);

      const customerMap = new Map();
      expandedExports.forEach(({ originalname, path: filePath }) => {
        const rows = readCSV(filePath);
        if (!rows.length) return;
        const custHeaders = Object.keys(rows[0]);
        const custEmailCol = detectEmailCol(custHeaders);
        rows.forEach(r => {
          const email = normalizeEmail(r[custEmailCol]);
          if (!email || customerMap.has(email)) return;
          customerMap.set(email, { ...r, _source: originalname });
        });
      });

      stream.progress(`Parsed ${customerMap.size.toLocaleString()} customer records — matching…`);

      const CUSTOMER_COLS = [
        'Customer ID', 'First Name', 'Last Name', 'Email',
        'Accepts Email Marketing', 'Phone', 'Accepts SMS Marketing',
        'Default Address Company', 'Default Address Address1', 'Default Address Address2',
        'Default Address City', 'Default Address Province Code',
        'Default Address Country Code', 'Default Address Zip',
        'Default Address Phone', 'Total Spent', 'Total Orders',
        'Note', 'Tax Exempt', 'Tags',
      ];

      const results = [];
      emailMap.forEach((listData, email) => {
        const cust = customerMap.get(email) || {};
        const matched = customerMap.has(email);
        const row = { email, matched, sales: listData._sales, source: cust._source || '' };
        Object.entries(listData).forEach(([k, v]) => {
          if (k !== '_sales') row[k.replace(/^_list_/, '')] = v;
        });
        CUSTOMER_COLS.forEach(col => {
          row[col] = matched ? (cust[col] || '') : (col === 'Email' ? email : '');
        });
        results.push(row);
      });

      results.sort((a, b) => {
        if (a.matched !== b.matched) return a.matched ? -1 : 1;
        return (parseFloat(b['Total Spent']) || 0) - (parseFloat(a['Total Spent']) || 0);
      });

      const matchedCount = results.filter(r => r.matched).length;
      stream.result({ total: results.length, matched: matchedCount, unmatched: results.length - matchedCount, results });

    } catch (err) {
      console.error(err);
      stream.error(err.message);
    } finally {
      uploadedPaths.forEach(p => { try { fs.unlinkSync(p); } catch (_) {} });
    }
  }
);

// ── POST /api/orders ───────────────────────────────────────────────────────────

app.post(
  '/api/orders',
  upload.any(),
  (req, res) => {
    const uploadedPaths = [];
    const stream = makeStreamer(res);
    try {
      const filesByField = groupFilesByField(req.files);
      const emailFileInfo = filesByField['emailFile']?.[0];
      const orderFileInfos = filesByField['orderFiles'] || [];

      if (!emailFileInfo) return stream.error('No email list file uploaded.');
      if (!orderFileInfos.length) return stream.error('No order files uploaded.');

      stream.progress('Parsing email list…');
      const expandedOrders = expandFiles(orderFileInfos);
      uploadedPaths.push(emailFileInfo.path, ...orderFileInfos.map(f => f.path), ...expandedOrders.filter(f => f._temp).map(f => f.path));

      const emailRows = readCSV(emailFileInfo.path);
      const emailHeaders = Object.keys(emailRows[0] || {});
      const emailCol = detectEmailCol(emailHeaders);
      const emailSet = new Set(emailRows.map(r => normalizeEmail(r[emailCol])).filter(Boolean));

      stream.progress(`Loaded ${emailSet.size.toLocaleString()} target emails — scanning order files…`);
      const orderMap = new Map();
      expandedOrders.forEach(({ originalname, path: filePath }) => {
        const rows = readCSV(filePath);
        rows.forEach(r => {
          const id = (r['Name'] || '').trim();
          const email = normalizeEmail(r['Email']);
          if (!id || !email || !emailSet.has(email)) return;
          if (orderMap.has(id)) return; // already recorded this order
          const total = parseFloat(r['Total']);
          if (!isNaN(total) && total > 0) {
            const tags = r['Tags'] || '';
            const subType = tags.includes('Subscription First Order') ? 'first'
                          : tags.includes('Subscription Recurring Order') ? 'recurring'
                          : 'none';
            orderMap.set(id, {
              id,
              email,
              total,
              createdAt: r['Created at'] || '',
              financialStatus: r['Financial Status'] || '',
              fulfillmentStatus: r['Fulfillment Status'] || '',
              currency: r['Currency'] || 'USD',
              source: originalname,
              subType,
            });
          }
        });
      });

      const orders = Array.from(orderMap.values());

      stream.progress(`Parsed ${orders.length.toLocaleString()} orders — computing stats…`);

      // Emails in the list that have zero orders in the exports
      const emailsWithOrders = new Set(orders.map(o => o.email));
      const unmatchedEmails = Array.from(emailSet)
        .filter(e => e && !emailsWithOrders.has(e))
        .sort();

      // Parse createdAt → JS Date, build trend buckets
      orders.forEach(o => {
        // "2026-05-18 07:30:22 -0700" → strip tz suffix for parsing
        const d = new Date(o.createdAt.replace(/ [+-]\d{4}$/, ''));
        o.ts = isNaN(d) ? null : d.getTime();
        o.dateStr = isNaN(d) ? null : d.toISOString().slice(0, 10);
      });

      const validOrders = orders.filter(o => o.ts !== null);
      validOrders.sort((a, b) => a.ts - b.ts);

      // ── Bucket builder ────────────────────────────────────────────────────────
      function dateKey(ts, granularity) {
        const d = new Date(ts);
        if (granularity === 'day') return d.toISOString().slice(0, 10);
        if (granularity === 'week') {
          const day = d.getDay() || 7;
          const mon = new Date(d);
          mon.setDate(d.getDate() - (day - 1));
          return mon.toISOString().slice(0, 10);
        }
        return d.toISOString().slice(0, 7);
      }

      function toBuckets(granularity) {
        const map = new Map();
        validOrders.forEach(o => {
          const key = dateKey(o.ts, granularity);
          if (!map.has(key)) map.set(key, { date: key, total: 0, count: 0 });
          const b = map.get(key);
          b.total = Math.round((b.total + o.total) * 100) / 100;
          b.count += 1;
        });
        return Array.from(map.values()).sort((a, b) => a.date.localeCompare(b.date));
      }

      // ── Subscription buckets (stacked revenue + trend) ────────────────────────
      function toSubBuckets(granularity) {
        const map = new Map();
        const zero = () => ({ date: '', subRevenue: 0, recurRevenue: 0, nonSubRevenue: 0, firstCount: 0, recurCount: 0, nonSubCount: 0 });
        validOrders.forEach(o => {
          const key = dateKey(o.ts, granularity);
          if (!map.has(key)) { const b = zero(); b.date = key; map.set(key, b); }
          const b = map.get(key);
          const r = Math.round(o.total * 100) / 100;
          if (o.subType === 'first')      { b.subRevenue  = Math.round((b.subRevenue  + r) * 100) / 100; b.firstCount++; }
          else if (o.subType === 'recurring') { b.recurRevenue = Math.round((b.recurRevenue + r) * 100) / 100; b.recurCount++; }
          else                            { b.nonSubRevenue = Math.round((b.nonSubRevenue + r) * 100) / 100; b.nonSubCount++; }
        });
        return Array.from(map.values()).sort((a, b) => a.date.localeCompare(b.date));
      }

      // ── Subscription summary stats ────────────────────────────────────────────
      const subFirst     = validOrders.filter(o => o.subType === 'first');
      const subRecurring = validOrders.filter(o => o.subType === 'recurring');
      const nonSub       = validOrders.filter(o => o.subType === 'none');

      const round2 = n => Math.round(n * 100) / 100;
      const sum    = arr => round2(arr.reduce((s, o) => s + o.total, 0));
      const aov    = arr => arr.length ? round2(sum(arr) / arr.length) : 0;

      const subRevenue    = sum(subFirst) + sum(subRecurring);
      const nonSubRevenue = sum(nonSub);
      const totalRevenue  = round2(validOrders.reduce((s, o) => s + o.total, 0));

      // Unique subscriber emails (those who placed a first-order)
      const uniqueSubscriberEmails = new Set(subFirst.map(o => o.email));
      // Avg recurring orders per subscriber
      const avgRecurPerSub = uniqueSubscriberEmails.size
        ? round2(subRecurring.length / uniqueSubscriberEmails.size)
        : 0;

      const subStats = {
        totalSubOrders:      subFirst.length + subRecurring.length,
        newSubscribers:      subFirst.length,
        recurringOrders:     subRecurring.length,
        nonSubOrders:        nonSub.length,
        subRevenue:          round2(subRevenue),
        nonSubRevenue:       round2(nonSubRevenue),
        subRevenuePercent:   totalRevenue > 0 ? round2(subRevenue / totalRevenue * 100) : 0,
        uniqueSubscribers:   uniqueSubscriberEmails.size,
        avgRecurPerSub,
        firstAOV:            aov(subFirst),
        recurAOV:            aov(subRecurring),
        nonSubAOV:           aov(nonSub),
        subByDay:            toSubBuckets('day'),
        subByWeek:           toSubBuckets('week'),
        subByMonth:          toSubBuckets('month'),
      };

      stream.result({
        totalOrders: validOrders.length,
        totalRevenue,
        uniqueCustomers: new Set(validOrders.map(o => o.email)).size,
        orders: validOrders,
        byDay: toBuckets('day'),
        byWeek: toBuckets('week'),
        byMonth: toBuckets('month'),
        subStats,
        hasSubscriptions: subStats.totalSubOrders > 0,
        unmatchedEmails,
        unmatchedCount: unmatchedEmails.length,
      });

    } catch (err) {
      console.error(err);
      stream.error(err.message);
    } finally {
      uploadedPaths.forEach(p => { try { fs.unlinkSync(p); } catch (_) {} });
    }
  }
);

app.use((err, req, res, next) => {
  if (err && err.name === 'MulterError') {
    console.error(`MulterError: ${err.message} (field: ${err.field})`);
    return res.status(400).json({ error: `Upload error: ${err.message}${err.field ? ` (field: ${err.field})` : ''}` });
  }
  next(err);
});

app.listen(PORT, () => {
  console.log(`\n  Customer Lookup running at http://localhost:${PORT}\n`);
});
