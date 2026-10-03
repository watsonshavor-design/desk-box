/* Desk immersive space. Self-contained WebGL + WebXR. No network, no trading calls. */
(function (root) {
  "use strict";

  function clean(value, fallback) {
    var t = String(value == null ? "" : value).replace(/\s+/g, " ").trim();
    if (!t || t === "…" || t === "—" || t === "-" || t === "..." || t === "..") return fallback;
    return t;
  }

  function textOf(id) {
    if (typeof document === "undefined") return "";
    var el = document.getElementById(id);
    return el ? el.textContent : "";
  }

  function readLive() {
    var session = clean(textOf("sessionClock"), "unavailable");
    var equity = clean(textOf("acctEquity"), "unavailable");
    var cash = clean(textOf("acctCash"), "unavailable");
    var distance = clean(textOf("acctDistance"), "unavailable");
    var floor = clean(textOf("acctFloor"), "");
    var temp = clean(textOf("homeTemp"), "unavailable");
    var kalaEl = typeof document !== "undefined"
      ? document.querySelector("#homeDeskCard .home-desk-label")
      : null;
    var kala = clean(kalaEl ? kalaEl.textContent : "", "unavailable");
    return {
      session: session,
      equity: equity,
      cash: cash,
      distance: distance,
      floor: floor,
      temp: temp,
      kala: kala
    };
  }

  function mat4Multiply(a, b, out) {
    var a00 = a[0], a01 = a[1], a02 = a[2], a03 = a[3];
    var a10 = a[4], a11 = a[5], a12 = a[6], a13 = a[7];
    var a20 = a[8], a21 = a[9], a22 = a[10], a23 = a[11];
    var a30 = a[12], a31 = a[13], a32 = a[14], a33 = a[15];
    var b0, b1, b2, b3;
    for (var i = 0; i < 4; i++) {
      b0 = b[i * 4]; b1 = b[i * 4 + 1]; b2 = b[i * 4 + 2]; b3 = b[i * 4 + 3];
      out[i * 4] = a00 * b0 + a10 * b1 + a20 * b2 + a30 * b3;
      out[i * 4 + 1] = a01 * b0 + a11 * b1 + a21 * b2 + a31 * b3;
      out[i * 4 + 2] = a02 * b0 + a12 * b1 + a22 * b2 + a32 * b3;
      out[i * 4 + 3] = a03 * b0 + a13 * b1 + a23 * b2 + a33 * b3;
    }
    return out;
  }

  function invertRigid(m, out) {
    out[0] = m[0]; out[1] = m[4]; out[2] = m[8]; out[3] = 0;
    out[4] = m[1]; out[5] = m[5]; out[6] = m[9]; out[7] = 0;
    out[8] = m[2]; out[9] = m[6]; out[10] = m[10]; out[11] = 0;
    var x = m[12], y = m[13], z = m[14];
    out[12] = -(out[0] * x + out[4] * y + out[8] * z);
    out[13] = -(out[1] * x + out[5] * y + out[9] * z);
    out[14] = -(out[2] * x + out[6] * y + out[10] * z);
    out[15] = 1;
    return out;
  }

  function panelMatrix(panel, out) {
    var c = Math.cos(panel.yaw), s = Math.sin(panel.yaw);
    var sx = panel.w, sy = panel.h, sz = 1;
    out[0] = c * sx; out[1] = 0; out[2] = -s * sx; out[3] = 0;
    out[4] = 0; out[5] = sy; out[6] = 0; out[7] = 0;
    out[8] = s * sz; out[9] = 0; out[10] = c * sz; out[11] = 0;
    out[12] = panel.x; out[13] = panel.y; out[14] = panel.z; out[15] = 1;
    return out;
  }

  function hitPanel(ray, panel) {
    var c = Math.cos(panel.yaw), s = Math.sin(panel.yaw);
    var nx = s, ny = 0, nz = c;
    var denom = ray.dx * nx + ray.dy * ny + ray.dz * nz;
    if (Math.abs(denom) < 1e-5) return null;
    var t = ((panel.x - ray.ox) * nx + (panel.y - ray.oy) * ny + (panel.z - ray.oz) * nz) / denom;
    if (t < 0.08 || t > 8) return null;
    var dx = ray.ox + ray.dx * t - panel.x;
    var dy = ray.oy + ray.dy * t - panel.y;
    var dz = ray.oz + ray.dz * t - panel.z;
    var lx = c * dx - s * dz;
    var ly = dy;
    if (Math.abs(lx) <= panel.w * 0.5 && Math.abs(ly) <= panel.h * 0.5) return t;
    return null;
  }

  function rayForward(matrix) {
    var dx = -matrix[8], dy = -matrix[9], dz = -matrix[10];
    var len = Math.hypot(dx, dy, dz) || 1;
    return {
      ox: matrix[12], oy: matrix[13], oz: matrix[14],
      dx: dx / len, dy: dy / len, dz: dz / len
    };
  }

  function beamMatrix(ray, length, out) {
    var fx = ray.dx, fy = ray.dy, fz = ray.dz;
    var rx = -fz, ry = 0, rz = fx;
    var rl = Math.hypot(rx, rz);
    if (rl < 1e-4) { rx = 1; rz = 0; rl = 1; }
    rx /= rl; rz /= rl;
    var ux = fy * rz - fz * ry;
    var uy = fz * rx - fx * rz;
    var uz = fx * ry - fy * rx;
    var ul = Math.hypot(ux, uy, uz) || 1;
    ux /= ul; uy /= ul; uz /= ul;
    out[0] = rx; out[1] = ry; out[2] = rz; out[3] = 0;
    out[4] = ux; out[5] = uy; out[6] = uz; out[7] = 0;
    out[8] = -fx * length; out[9] = -fy * length; out[10] = -fz * length; out[11] = 0;
    out[12] = ray.ox; out[13] = ray.oy; out[14] = ray.oz; out[15] = 1;
    return out;
  }

  function roundRect(ctx, x, y, w, h, r) {
    var rr = Math.min(r, w * 0.5, h * 0.5);
    ctx.beginPath();
    ctx.moveTo(x + rr, y);
    ctx.arcTo(x + w, y, x + w, y + h, rr);
    ctx.arcTo(x + w, y + h, x, y + h, rr);
    ctx.arcTo(x, y + h, x, y, rr);
    ctx.arcTo(x, y, x + w, y, rr);
    ctx.closePath();
  }

  function paintGlass(canvas, spec) {
    var ctx = canvas.getContext("2d");
    var w = canvas.width, h = canvas.height;
    ctx.clearRect(0, 0, w, h);
    roundRect(ctx, 12, 12, w - 24, h - 24, 42);
    var g = ctx.createLinearGradient(0, 0, w, h);
    g.addColorStop(0, spec.hot ? "rgba(16, 42, 58, 0.78)" : "rgba(12, 20, 38, 0.70)");
    g.addColorStop(1, "rgba(6, 10, 22, 0.58)");
    ctx.fillStyle = g;
    ctx.fill();
    ctx.lineWidth = spec.hot ? 14 : 8;
    ctx.strokeStyle = spec.accent || (spec.hot ? "rgba(130, 255, 226, 0.95)" : "rgba(156, 196, 255, 0.62)");
    ctx.stroke();
    ctx.font = "600 40px system-ui, sans-serif";
    ctx.fillStyle = spec.kickerColor || "#8fd6ff";
    ctx.fillText(spec.kicker, 64, 100);
    var y = 196;
    var lines = spec.lines || [];
    for (var i = 0; i < lines.length; i++) {
      var big = i === 0;
      ctx.font = (big ? "700 84px " : "650 46px ") + "system-ui, sans-serif";
      ctx.fillStyle = lines[i].dim ? "rgba(186, 198, 214, 0.8)" : (big ? "#ffffff" : "rgba(226, 236, 248, 0.94)");
      var label = typeof lines[i] === "string" ? lines[i] : lines[i].text;
      ctx.fillText(label, 64, y);
      y += big ? 108 : 70;
    }
    if (spec.dwell > 0.02) {
      ctx.fillStyle = "rgba(130, 255, 226, 0.9)";
      roundRect(ctx, 64, h - 64, (w - 128) * Math.max(0, Math.min(1, spec.dwell)), 12, 6);
      ctx.fill();
    }
  }

  function sphereMesh(latBands, lonBands) {
    var pos = [], uv = [], idx = [];
    for (var lat = 0; lat <= latBands; lat++) {
      var theta = lat * Math.PI / latBands;
      var v = lat / latBands;
      for (var lon = 0; lon <= lonBands; lon++) {
        var phi = lon * 2 * Math.PI / lonBands;
        var u = lon / lonBands;
        var x = Math.sin(theta) * Math.cos(phi);
        var y = Math.cos(theta);
        var z = Math.sin(theta) * Math.sin(phi);
        pos.push(x, y, z);
        uv.push(u, v);
      }
    }
    for (var a = 0; a < latBands; a++) {
      for (var b = 0; b < lonBands; b++) {
        var first = a * (lonBands + 1) + b;
        var second = first + lonBands + 1;
        idx.push(first, second, first + 1, second, second + 1, first + 1);
      }
    }
    return { pos: new Float32Array(pos), uv: new Float32Array(uv), idx: new Uint16Array(idx) };
  }

  function quadMesh() {
    return {
      pos: new Float32Array([
        -0.5, -0.5, 0, 0.5, -0.5, 0, 0.5, 0.5, 0, -0.5, 0.5, 0
      ]),
      uv: new Float32Array([0, 1, 1, 1, 1, 0, 0, 0]),
      idx: new Uint16Array([0, 1, 2, 0, 2, 3])
    };
  }

  function floorMesh() {
    return {
      pos: new Float32Array([
        -1, 0, -1, 1, 0, -1, 1, 0, 1, -1, 0, 1
      ]),
      uv: new Float32Array([0, 0, 1, 0, 1, 1, 0, 1]),
      idx: new Uint16Array([0, 2, 1, 0, 3, 2])
    };
  }

  function beamMesh() {
    var s = 0.0045;
    return {
      pos: new Float32Array([
        -s, -s, 0, s, -s, 0, s, s, 0, -s, s, 0,
        -s, -s, -1, s, -s, -1, s, s, -1, -s, s, -1
      ]),
      uv: new Float32Array([0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1]),
      idx: new Uint16Array([
        0, 1, 2, 0, 2, 3,
        4, 6, 5, 4, 7, 6,
        0, 4, 5, 0, 5, 1,
        1, 5, 6, 1, 6, 2,
        2, 6, 7, 2, 7, 3,
        3, 7, 4, 3, 4, 0
      ])
    };
  }

  function ribbonMesh() {
    var seg = 48;
    var pos = [], uv = [], idx = [];
    for (var i = 0; i <= seg; i++) {
      var t = i / seg;
      var ang = -0.9 + t * 2.4;
      var radius = 6.2 + Math.sin(t * Math.PI) * 1.4;
      var x = Math.sin(ang) * radius;
      var z = -Math.cos(ang) * radius * 0.72;
      var y = 1.15 + Math.sin(t * Math.PI * 2.0) * 0.85;
      pos.push(x, y, z, x, y + 1.15, z);
      uv.push(t, 0, t, 1);
    }
    for (var k = 0; k < seg; k++) {
      var a0 = k * 2;
      idx.push(a0, a0 + 1, a0 + 2, a0 + 1, a0 + 3, a0 + 2);
    }
    return { pos: new Float32Array(pos), uv: new Float32Array(uv), idx: new Uint16Array(idx) };
  }

  function motePositions(floor) {
    var out = [];
    var base = floor ? 0.3 : -1.2;
    for (var i = 0; i < 160; i++) {
      var ang = (i * 2.399) % (Math.PI * 2);
      var radius = 1.8 + (i % 17) * 0.38;
      var y = base + ((i * 47) % 100) / 28;
      out.push(Math.cos(ang) * radius, y, -Math.abs(Math.sin(ang)) * radius - 0.6);
    }
    return new Float32Array(out);
  }

  var SKY_VS = [
    "precision mediump float;",
    "attribute vec3 aPos;",
    "uniform mat4 uMVP;",
    "varying vec3 vPos;",
    "void main(){",
    "  vPos = aPos;",
    "  gl_Position = uMVP * vec4(aPos, 1.0);",
    "}"
  ].join("\n");

  var SKY_FS = [
    "precision mediump float;",
    "varying vec3 vPos;",
    "uniform float uTime;",
    "void main(){",
    "  vec3 p = normalize(vPos);",
    "  float h = p.y;",
    "  float wave = sin(p.x * 6.0 + uTime * 0.65) * sin(p.z * 4.5 - uTime * 0.38);",
    "  float curtain = smoothstep(-0.2, 0.45, h) * (0.28 + 0.72 * (0.5 + 0.5 * wave));",
    "  curtain *= smoothstep(0.95, 0.25, h);",
    "  vec3 base = mix(vec3(0.012, 0.016, 0.04), vec3(0.035, 0.05, 0.09), smoothstep(-0.35, 0.55, h));",
    "  vec3 teal = vec3(0.05, 0.82, 0.70);",
    "  vec3 violet = vec3(0.40, 0.16, 0.82);",
    "  vec3 rose = vec3(0.78, 0.22, 0.48);",
    "  float blend = 0.5 + 0.5 * sin(p.x * 3.2 + uTime * 0.28);",
    "  vec3 aur = mix(violet, teal, blend);",
    "  aur = mix(aur, rose, 0.22 + 0.22 * sin(uTime * 0.18 + p.z * 3.4));",
    "  vec3 col = base + aur * curtain * 0.9;",
    "  float seed = fract(sin(dot(floor(p.xy * 170.0), vec2(12.9898, 78.233))) * 43758.5453);",
    "  float star = step(0.993, seed) * smoothstep(0.05, 0.7, h);",
    "  col += vec3(star * 0.85);",
    "  gl_FragColor = vec4(col, 1.0);",
    "}"
  ].join("\n");

  var FLOOR_VS = [
    "precision mediump float;",
    "attribute vec3 aPos;",
    "uniform mat4 uMVP;",
    "uniform mat4 uModel;",
    "varying vec3 vWorld;",
    "void main(){",
    "  vec4 w = uModel * vec4(aPos, 1.0);",
    "  vWorld = w.xyz;",
    "  gl_Position = uMVP * vec4(aPos, 1.0);",
    "}"
  ].join("\n");

  var FLOOR_FS = [
    "precision mediump float;",
    "varying vec3 vWorld;",
    "uniform float uFloorY;",
    "void main(){",
    "  vec2 g = abs(fract(vWorld.xz) - 0.5);",
    "  float line = 1.0 - smoothstep(0.0, 0.035, min(g.x, g.y));",
    "  float fade = exp(-length(vWorld.xz) * 0.11);",
    "  vec3 col = mix(vec3(0.015, 0.02, 0.04), vec3(0.10, 0.55, 0.62), line);",
    "  float ring = abs(length(vWorld.xz) - 2.15);",
    "  float halo = 1.0 - smoothstep(0.0, 0.08, ring);",
    "  col += vec3(0.15, 0.45, 0.85) * halo * 0.45;",
    "  gl_FragColor = vec4(col, 0.92 * fade + 0.08);",
    "}"
  ].join("\n");

  var PANEL_VS = [
    "precision mediump float;",
    "attribute vec3 aPos;",
    "attribute vec2 aUv;",
    "uniform mat4 uMVP;",
    "varying vec2 vUv;",
    "void main(){",
    "  vUv = aUv;",
    "  gl_Position = uMVP * vec4(aPos, 1.0);",
    "}"
  ].join("\n");

  var PANEL_FS = [
    "precision mediump float;",
    "varying vec2 vUv;",
    "uniform sampler2D uTex;",
    "void main(){",
    "  gl_FragColor = texture2D(uTex, vUv);",
    "}"
  ].join("\n");

  var ADD_VS = [
    "precision mediump float;",
    "attribute vec3 aPos;",
    "attribute vec2 aUv;",
    "uniform mat4 uMVP;",
    "uniform float uTime;",
    "uniform float uPhase;",
    "varying vec2 vUv;",
    "varying float vH;",
    "void main(){",
    "  vUv = aUv;",
    "  vec3 p = aPos;",
    "  p.y += sin(uTime * 0.8 + aUv.x * 8.0 + uPhase) * 0.28;",
    "  vH = aUv.y;",
    "  gl_Position = uMVP * vec4(p, 1.0);",
    "}"
  ].join("\n");

  var ADD_FS = [
    "precision mediump float;",
    "varying vec2 vUv;",
    "varying float vH;",
    "uniform float uTime;",
    "uniform vec3 uColor;",
    "void main(){",
    "  float edge = smoothstep(0.0, 0.25, vH) * smoothstep(1.0, 0.65, vH);",
    "  float pulse = 0.45 + 0.55 * sin(uTime * 1.3 + vUv.x * 10.0);",
    "  float alpha = edge * pulse * 0.42;",
    "  gl_FragColor = vec4(uColor, alpha);",
    "}"
  ].join("\n");

  var MOTE_VS = [
    "precision mediump float;",
    "attribute vec3 aPos;",
    "uniform mat4 uMVP;",
    "uniform float uTime;",
    "void main(){",
    "  vec3 p = aPos;",
    "  p.y += sin(uTime * 0.6 + aPos.x) * 0.08;",
    "  gl_Position = uMVP * vec4(p, 1.0);",
    "  gl_PointSize = 5.0;",
    "}"
  ].join("\n");

  var MOTE_FS = [
    "precision mediump float;",
    "uniform vec3 uColor;",
    "void main(){",
    "  vec2 c = gl_PointCoord - vec2(0.5);",
    "  float d = dot(c, c);",
    "  if (d > 0.25) discard;",
    "  float a = smoothstep(0.25, 0.0, d);",
    "  gl_FragColor = vec4(uColor, a * 0.85);",
    "}"
  ].join("\n");

  var BEAM_FS = [
    "precision mediump float;",
    "uniform vec3 uColor;",
    "void main(){ gl_FragColor = vec4(uColor, 0.85); }"
  ].join("\n");

  function compile(gl, type, src) {
    var sh = gl.createShader(type);
    gl.shaderSource(sh, src);
    gl.compileShader(sh);
    if (!gl.getShaderParameter(sh, gl.COMPILE_STATUS)) {
      var log = gl.getShaderInfoLog(sh) || "shader";
      gl.deleteShader(sh);
      throw new Error(log);
    }
    return sh;
  }

  function program(gl, vs, fs, withUv) {
    var p = gl.createProgram();
    gl.attachShader(p, compile(gl, gl.VERTEX_SHADER, vs));
    gl.attachShader(p, compile(gl, gl.FRAGMENT_SHADER, fs));
    gl.bindAttribLocation(p, 0, "aPos");
    if (withUv) gl.bindAttribLocation(p, 1, "aUv");
    gl.linkProgram(p);
    if (!gl.getProgramParameter(p, gl.LINK_STATUS)) {
      throw new Error(gl.getProgramInfoLog(p) || "link");
    }
    return p;
  }

  function uploadMesh(gl, mesh) {
    var pos = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, pos);
    gl.bufferData(gl.ARRAY_BUFFER, mesh.pos, gl.STATIC_DRAW);
    var uv = null;
    if (mesh.uv) {
      uv = gl.createBuffer();
      gl.bindBuffer(gl.ARRAY_BUFFER, uv);
      gl.bufferData(gl.ARRAY_BUFFER, mesh.uv, gl.STATIC_DRAW);
    }
    var idx = gl.createBuffer();
    gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, idx);
    gl.bufferData(gl.ELEMENT_ARRAY_BUFFER, mesh.idx, gl.STATIC_DRAW);
    return { pos: pos, uv: uv, idx: idx, count: mesh.idx.length };
  }

  function bindMesh(gl, gpu, withUv) {
    gl.bindBuffer(gl.ARRAY_BUFFER, gpu.pos);
    gl.enableVertexAttribArray(0);
    gl.vertexAttribPointer(0, 3, gl.FLOAT, false, 0, 0);
    if (withUv && gpu.uv) {
      gl.bindBuffer(gl.ARRAY_BUFFER, gpu.uv);
      gl.enableVertexAttribArray(1);
      gl.vertexAttribPointer(1, 2, gl.FLOAT, false, 0, 0);
    }
    gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, gpu.idx);
  }

  function makeTexture(gl, canvas) {
    var tex = gl.createTexture();
    gl.bindTexture(gl.TEXTURE_2D, tex);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, canvas);
    return tex;
  }

  function refreshTexture(gl, tex, canvas) {
    gl.bindTexture(gl.TEXTURE_2D, tex);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, canvas);
  }

  function lineObj(text, dim) {
    return { text: text, dim: !!dim || text === "unavailable" || text === "" };
  }

  function buildSpecs(live, hotId, dwell) {
    return [
      {
        id: "session",
        kicker: "SESSION",
        lines: [lineObj(live.session, live.session === "unavailable"), lineObj(live.kala, live.kala === "unavailable")],
        w: 1.15, h: 0.82, x: -1.28, yaw: 0.42, z: -2.15
      },
      {
        id: "account",
        kicker: "ACCOUNT",
        lines: [
          lineObj(live.equity === "unavailable" ? "Equity unavailable" : ("Equity " + live.equity), live.equity === "unavailable"),
          lineObj(live.cash === "unavailable" ? "Cash unavailable" : ("Cash " + live.cash), live.cash === "unavailable"),
          lineObj(live.distance === "unavailable" ? "Floor distance unavailable" : ("Floor distance " + live.distance), live.distance === "unavailable"),
          lineObj(live.floor || "unavailable", !live.floor)
        ],
        w: 1.42, h: 1.05, x: 0, yaw: 0, z: -2.02
      },
      {
        id: "taylors",
        kicker: "TAYLORS",
        lines: [lineObj(live.temp, live.temp === "unavailable"), lineObj("temperature", false)],
        w: 1.05, h: 0.78, x: 1.28, yaw: -0.42, z: -2.15
      },
      {
        id: "exit",
        kicker: "LEAVE THE ROOM",
        kickerColor: "#ffb4b8",
        accent: hotId === "exit" ? "rgba(255, 140, 150, 0.95)" : "rgba(255, 120, 136, 0.7)",
        lines: [lineObj("Exit", false)],
        dwell: hotId === "exit" ? dwell : 0,
        w: 0.72, h: 0.36, x: 0, yaw: 0, z: -1.55, low: true
      }
    ];
  }

  function layoutPanels(specs, floor) {
    var eye = floor ? 1.42 : 0.0;
    specs.forEach(function (panel) {
      panel.y = panel.low ? (eye - 0.62) : eye;
    });
    return specs;
  }

  function signature(spec) {
    return spec.kicker + "|" + (spec.lines || []).map(function (l) { return l.text; }).join("|") + "|" + (spec.hot ? 1 : 0) + "|" + Math.round((spec.dwell || 0) * 20);
  }

  var api = {
    readLive: readLive,
    hitPanel: hitPanel,
    rayForward: rayForward
  };

  function setNote(msg) {
    var note = document.getElementById("enterVrNote");
    if (note) note.textContent = msg || "";
  }

  function markNoXr(btn) {
    btn.textContent = "Headset browser required";
    btn.disabled = true;
    btn.setAttribute("aria-disabled", "true");
  }

  function enter(btn) {
    if (!root.navigator || !root.navigator.xr || typeof root.navigator.xr.requestSession !== "function") {
      markNoXr(btn);
      return;
    }
    btn.disabled = true;
    btn.textContent = "Entering VR";
    setNote("");
    var canvas = document.createElement("canvas");
    canvas.setAttribute("aria-hidden", "true");
    canvas.style.cssText = "position:fixed;width:2px;height:2px;opacity:0;pointer-events:none;";
    document.body.appendChild(canvas);
    var gl = canvas.getContext("webgl", { alpha: false, antialias: true, xrCompatible: true });
    if (!gl) {
      canvas.parentNode.removeChild(canvas);
      btn.disabled = false;
      btn.textContent = "Enter VR";
      setNote("VR could not start in this browser.");
      return;
    }
    var session = null;
    var ended = false;
    function fail(message) {
      if (ended) return;
      ended = true;
      try { if (session) session.end(); } catch (e) {}
      if (canvas.parentNode) canvas.parentNode.removeChild(canvas);
      btn.disabled = false;
      if (!root.navigator.xr) markNoXr(btn);
      else btn.textContent = "Enter VR";
      setNote(message || "VR could not start. Stay in the headset browser and try again.");
    }
    root.navigator.xr.requestSession("immersive-vr", {
      optionalFeatures: ["local-floor", "bounded-floor", "hand-tracking"]
    }).then(function (next) {
      session = next;
      return gl.makeXRCompatible().then(function () {
        var layer;
        try {
          layer = new XRWebGLLayer(session, gl, { antialias: true });
        } catch (e) {
          layer = new XRWebGLLayer(session, gl);
        }
        session.updateRenderState({ baseLayer: layer });
        var ask = session.requestReferenceSpace("local-floor").then(function (space) {
          return { space: space, floor: true };
        }, function () {
          return session.requestReferenceSpace("local").then(function (space) {
            return { space: space, floor: false };
          });
        });
        return ask.then(function (ref) {
          start(session, gl, canvas, btn, ref.space, ref.floor, function () { ended = true; });
        });
      });
    }).catch(function () {
      fail("VR could not start. Stay in the headset browser and try again.");
    });
  }

  function start(session, gl, canvas, btn, refSpace, floor, onEnd) {
    var sky = program(gl, SKY_VS, SKY_FS, false);
    var floorProg = program(gl, FLOOR_VS, FLOOR_FS, false);
    var panelProg = program(gl, PANEL_VS, PANEL_FS, true);
    var addProg = program(gl, ADD_VS, ADD_FS, true);
    var moteProg = program(gl, MOTE_VS, MOTE_FS, false);
    var beamProg = program(gl, PANEL_VS, BEAM_FS, false);
    var skyGpu = uploadMesh(gl, sphereMesh(18, 28));
    var floorGpu = uploadMesh(gl, floorMesh());
    var quadGpu = uploadMesh(gl, quadMesh());
    var beamGpu = uploadMesh(gl, beamMesh());
    var ribbonA = uploadMesh(gl, ribbonMesh());
    var moteBuf = gl.createBuffer();
    var motes = motePositions(floor);
    gl.bindBuffer(gl.ARRAY_BUFFER, moteBuf);
    gl.bufferData(gl.ARRAY_BUFFER, motes, gl.STATIC_DRAW);

    var reduce = false;
    try {
      reduce = root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch (e) {}

    var panels = layoutPanels(buildSpecs(readLive(), "", 0), floor);
    panels.forEach(function (panel) {
      panel.canvas = document.createElement("canvas");
      panel.canvas.width = panel.id === "account" ? 1280 : (panel.id === "exit" ? 900 : 1100);
      panel.canvas.height = panel.id === "exit" ? 420 : 720;
      panel.hot = false;
      panel.dwell = 0;
      paintGlass(panel.canvas, panel);
      panel.sig = signature(panel);
      panel.tex = makeTexture(gl, panel.canvas);
    });

    var hover = "";
    var dwell = 0;
    var last = 0;
    var view = new Float32Array(16);
    var model = new Float32Array(16);
    var mvp = new Float32Array(16);
    var mv = new Float32Array(16);
    var skyModel = new Float32Array([
      18, 0, 0, 0,
      0, 18, 0, 0,
      0, 0, 18, 0,
      0, floor ? 1.4 : 0, 0, 1
    ]);
    var floorModel = new Float32Array(16);
    var span = 14;
    floorModel[0] = span; floorModel[5] = 1; floorModel[10] = span; floorModel[15] = 1;
    floorModel[13] = floor ? 0 : -1.45;

    function redraw() {
      var live = readLive();
      var next = layoutPanels(buildSpecs(live, hover, dwell), floor);
      for (var i = 0; i < panels.length; i++) {
        var panel = panels[i];
        var spec = next[i];
        panel.lines = spec.lines;
        panel.kicker = spec.kicker;
        panel.hot = hover === panel.id;
        panel.dwell = panel.id === "exit" ? dwell : 0;
        panel.accent = spec.accent;
        panel.kickerColor = spec.kickerColor;
        var sig = signature(panel);
        if (sig !== panel.sig) {
          paintGlass(panel.canvas, panel);
          refreshTexture(gl, panel.tex, panel.canvas);
          panel.sig = sig;
        }
      }
    }

    function finish() {
      if (!alive) return;
      alive = false;
      onEnd();
      if (canvas.parentNode) canvas.parentNode.removeChild(canvas);
      btn.disabled = false;
      btn.textContent = "Enter VR";
      setNote("");
    }
    session.addEventListener("end", finish);
    var hoverSource = null;
    var hoverGaze = false;
    var alive = true;
    session.addEventListener("select", function (ev) {
      if (!alive || hover !== "exit") return;
      if (hoverGaze || ev.inputSource === hoverSource) session.end();
    });

    function drawIndexed(progName, gpu, withUv, proj) {
      mat4Multiply(view, model, mv);
      mat4Multiply(proj, mv, mvp);
      var loc = gl.getUniformLocation(progName, "uMVP");
      gl.uniformMatrix4fv(loc, false, mvp);
      bindMesh(gl, gpu, withUv);
      if (!withUv) gl.disableVertexAttribArray(1);
      gl.drawElements(gl.TRIANGLES, gpu.count, gl.UNSIGNED_SHORT, 0);
    }

    function frame(t, xrFrame) {
      if (!alive) return;
      session.requestAnimationFrame(frame);
      var pose = xrFrame.getViewerPose(refSpace);
      if (!pose) return;
      var dt = last ? Math.min(0.05, (t - last) / 1000) : 0;
      last = t;
      var time = reduce ? 0 : t * 0.001;
      var hits = [];
      var sources = session.inputSources || [];
      for (var i = 0; i < sources.length; i++) {
        var src = sources[i];
        if (!src.targetRaySpace) continue;
        var srcPose = xrFrame.getPose(src.targetRaySpace, refSpace);
        if (!srcPose) continue;
        var ray = rayForward(srcPose.transform.matrix);
        var best = null, bestT = 99, bestId = "";
        for (var p = 0; p < panels.length; p++) {
          var dist = hitPanel(ray, panels[p]);
          if (dist != null && dist < bestT) { bestT = dist; best = panels[p]; bestId = panels[p].id; }
        }
        hits.push({ ray: ray, t: best ? bestT : 2.4, id: bestId, source: src, gaze: false });
      }
      if (!hits.length && pose.views.length) {
        var rayG = rayForward(pose.views[0].transform.matrix);
        var bestG = null, bestTG = 99, idG = "";
        for (var g = 0; g < panels.length; g++) {
          var dg = hitPanel(rayG, panels[g]);
          if (dg != null && dg < bestTG) { bestTG = dg; bestG = panels[g]; idG = panels[g].id; }
        }
        hits.push({ ray: rayG, t: bestG ? bestTG : 2.2, id: idG, source: null, gaze: true });
      }
      var nextHover = "";
      hoverSource = null;
      hoverGaze = false;
      for (var h = 0; h < hits.length; h++) {
        if (hits[h].id) {
          nextHover = hits[h].id;
          hoverSource = hits[h].source;
          hoverGaze = hits[h].gaze;
          break;
        }
      }
      if (nextHover !== hover) {
        hover = nextHover;
        dwell = 0;
      } else if (hover === "exit" && hoverGaze) {
        dwell += dt;
        if (dwell >= 1.25) session.end();
      } else {
        dwell = 0;
      }
      redraw();

      var layer = session.renderState.baseLayer;
      gl.bindFramebuffer(gl.FRAMEBUFFER, layer.framebuffer);
      for (var v = 0; v < pose.views.length; v++) {
        var viewPose = pose.views[v];
        var vp = layer.getViewport(viewPose);
        gl.viewport(vp.x, vp.y, vp.width, vp.height);
        gl.scissor(vp.x, vp.y, vp.width, vp.height);
        gl.enable(gl.SCISSOR_TEST);
        gl.clearColor(0.012, 0.016, 0.035, 1);
        gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
        invertRigid(viewPose.transform.matrix, view);
        var proj = viewPose.projectionMatrix;
        gl.disable(gl.BLEND);
        gl.enable(gl.DEPTH_TEST);
        gl.depthMask(false);
        gl.useProgram(sky);
        gl.uniform1f(gl.getUniformLocation(sky, "uTime"), time);
        model.set(skyModel);
        drawIndexed(sky, skyGpu, false, proj);

        gl.depthMask(true);
        gl.clear(gl.DEPTH_BUFFER_BIT);
        gl.useProgram(floorProg);
        gl.uniform1f(gl.getUniformLocation(floorProg, "uFloorY"), floorModel[13]);
        var modelLoc = gl.getUniformLocation(floorProg, "uModel");
        model.set(floorModel);
        gl.uniformMatrix4fv(modelLoc, false, model);
        drawIndexed(floorProg, floorGpu, false, proj);

        gl.enable(gl.BLEND);
        gl.blendFunc(gl.SRC_ALPHA, gl.ONE);
        gl.depthMask(false);
        gl.useProgram(addProg);
        gl.uniform1f(gl.getUniformLocation(addProg, "uTime"), time);
        gl.uniform1f(gl.getUniformLocation(addProg, "uPhase"), 0);
        gl.uniform3f(gl.getUniformLocation(addProg, "uColor"), 0.25, 0.85, 0.75);
        model.set(skyModel);
        model[0] = 1; model[5] = 1; model[10] = 1;
        model[12] = 0; model[13] = floor ? 1.2 : -0.2; model[14] = -1.2;
        drawIndexed(addProg, ribbonA, true, proj);
        gl.uniform1f(gl.getUniformLocation(addProg, "uPhase"), 2.1);
        gl.uniform3f(gl.getUniformLocation(addProg, "uColor"), 0.45, 0.25, 0.95);
        model[12] = 0.4; model[13] = floor ? 1.8 : 0.4; model[14] = -1.6;
        drawIndexed(addProg, ribbonA, true, proj);

        gl.useProgram(moteProg);
        gl.uniform1f(gl.getUniformLocation(moteProg, "uTime"), time);
        gl.uniform3f(gl.getUniformLocation(moteProg, "uColor"), 0.65, 0.85, 1.0);
        mat4Multiply(view, model, mv);
        model[0] = 1; model[5] = 1; model[10] = 1; model[12] = 0; model[13] = 0; model[14] = 0; model[15] = 1;
        mat4Multiply(proj, view, mvp);
        gl.uniformMatrix4fv(gl.getUniformLocation(moteProg, "uMVP"), false, mvp);
        gl.bindBuffer(gl.ARRAY_BUFFER, moteBuf);
        gl.enableVertexAttribArray(0);
        gl.vertexAttribPointer(0, 3, gl.FLOAT, false, 0, 0);
        gl.disableVertexAttribArray(1);
        gl.drawArrays(gl.POINTS, 0, motes.length / 3);

        gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);
        gl.depthMask(true);
        gl.useProgram(panelProg);
        for (var n = 0; n < panels.length; n++) {
          panelMatrix(panels[n], model);
          gl.activeTexture(gl.TEXTURE0);
          gl.bindTexture(gl.TEXTURE_2D, panels[n].tex);
          gl.uniform1i(gl.getUniformLocation(panelProg, "uTex"), 0);
          drawIndexed(panelProg, quadGpu, true, proj);
        }

        gl.depthMask(false);
        gl.useProgram(beamProg);
        for (var b = 0; b < hits.length; b++) {
          if (hits[b].gaze) continue;
          var len = hits[b].t;
          beamMatrix(hits[b].ray, len, model);
          var hot = hits[b].id ? [0.5, 1.0, 0.85] : [0.35, 0.7, 1.0];
          gl.uniform3f(gl.getUniformLocation(beamProg, "uColor"), hot[0], hot[1], hot[2]);
          drawIndexed(beamProg, beamGpu, false, proj);
        }
        gl.depthMask(true);
        gl.disable(gl.SCISSOR_TEST);
      }
    }
    session.requestAnimationFrame(frame);
    btn.textContent = "In VR";
  }

  function bind() {
    var btn = document.getElementById("enterVr");
    if (!btn || btn.dataset.vrBound) return;
    btn.dataset.vrBound = "1";
    try {
      if (!root.navigator || !root.navigator.xr || typeof root.navigator.xr.isSessionSupported !== "function") {
        markNoXr(btn);
        return;
      }
      root.navigator.xr.isSessionSupported("immersive-vr").then(function (ok) {
        if (!ok) markNoXr(btn);
        else if (btn.textContent === "Enter VR") btn.disabled = false;
      }).catch(function () { markNoXr(btn); });
      btn.addEventListener("click", function () {
        if (btn.disabled && btn.textContent === "Headset browser required") return;
        enter(btn);
      });
    } catch (e) {
      markNoXr(btn);
    }
  }

  api.enter = enter;
  api.bind = bind;
  root.DeskVR = api;
  if (typeof document !== "undefined") {
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", bind);
    else bind();
  }
})(typeof window !== "undefined" ? window : globalThis);
