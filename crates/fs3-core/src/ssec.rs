//! SSE-C 分块 AES-256-GCM 加密原语(M11 E1-1;ADR-12 DE1)。
//!
//! - 分块:chunk = 64KiB([`SSE_CHUNK_SIZE`],与段 CRC 网格同粒度);
//! - 密钥:`data_key = HKDF-SHA256(customer_key, salt=None,
//!   info="fasts3-sse-c-v1")`([`SseCKey::data_key`]);客户密钥零落盘、
//!   不进审计/日志,[`SseCKey`] Drop 时 zeroize 擦除;
//! - nonce:ADR 原文 `HMAC(key, object_id ‖ chunk_no)`,落地为
//!   `HMAC-SHA256(data_key, nonce_base ‖ be64(chunk_no))` 取前 12B——
//!   nonce_base 即每对象随机 96bit 标识(等价 ADR 的 object_id 绑定),
//!   chunk_no 为大端 u64。确定性派生:重排/截断/跨对象重放 chunk 即
//!   认证失败;
//! - aad = nonce_base:GCM tag 显式认证对象标识(与 nonce 派生中的
//!   nonce_base 构成双重绑定);
//! - tag:每 chunk 16B GCM tag 存元数据;密文与明文**等长**,数据区
//!   长度不变。
//!
//! 补录(ADR-12 定向验证裁决):D-E5 = `SseCKey::key_md5` 校验子随
//! `SseInfo` 落盘,读路径错 key 判 400(不再等 GCM 失败);D-E6 =
//! multipart 分片 nonce_base 由 [`derive_part_nonce_base`] 确定性派生
//! (重传幂等,安全取舍见该函数文档)。
//!
//! SSE-S3(M11 K1-1,ADR-12 DS1):KEK/DEK 两级复用同一 64KiB 分块网格,
//! 数据密钥 = 每对象随机 DEK(非 HKDF 派生);KEK 按代派生、DEK 包裹/
//! 解包/换代重包裹与写密钥签发见本模块 `*_sse_s3_*` 组合原语(KEK 局部
//! 副本用完即擦,DEK 由 [`SseS3WriteKey`]/[`ChunkedGcm`] Drop 擦除)。
//!
//! chunk 数与对象大小自洽:chunk_no 占全 u64 空间;同一 nonce_base 下
//! 不同 chunk_no 的 HMAC 输入互异,nonce 碰撞只剩 96bit 截断输出的随机
//! 碰撞,概率由生日界定(n 个 chunk ≈ n²/2⁹⁷;1TiB 对象 = 2²⁴ chunk →
//! ≈2⁻⁴⁹),远低于任何实际部署上限。

use aes_gcm::aead::{AeadInPlace, KeyInit, Tag};
use aes_gcm::{Aes256Gcm, Nonce};
use hmac::{Hmac, Mac};
use md5::Digest as _;
use sha2::Sha256;
use zeroize::Zeroize;

type HmacSha256 = Hmac<Sha256>;

/// HKDF-SHA256 输出长度上限(RFC 5869 §2.3:255 × HashLen = 255 × 32)。
const HKDF_SHA256_MAX_LEN: usize = 255 * 32;

/// SSE-C data_key 派生 info(ADR-12 DE1 指定字符串)。
const SSE_C_HKDF_INFO: &[u8] = b"fasts3-sse-c-v1";

/// SSE-C 分块粒度(ADR-12 DE1:64KiB,复用段 CRC 网格粒度)。
pub const SSE_CHUNK_SIZE: usize = 64 * 1024;

/// SSE-C 原语错误(显式、不携带密钥/明文/密文内容)。
#[derive(Debug, thiserror::Error)]
pub enum SseError {
    /// SSE-C 客户密钥长度非 32B(AES-256 要求)。
    #[error("sse-c customer key must be 32 bytes, got {0}")]
    InvalidKeyLength(usize),

    /// GCM tag 认证失败:密文/tag 被篡改、chunk 错位重放/重排、或密钥
    /// 与对象标识不符。不区分具体失败原因(防预言机)。
    #[error("sse-c chunk authentication failed")]
    AuthenticationFailed,

    /// SSE-S3 wrapped_dek 长度不符(M11 K1-1;恒 12B nonce ‖ 32B ct ‖
    /// 16B tag = 60B;元数据损坏)。
    #[error("sse-s3 wrapped DEK must be 60 bytes, got {0}")]
    MalformedWrappedDek(usize),

    /// OS CSPRNG 失败(DEK/包裹 nonce 生成;极罕见,显式不 panic)。
    #[error("sse-s3 random source failure")]
    Rng,
}

/// HKDF-SHA256(RFC 5869 extract + expand;复用 hmac/sha2,不引新 crate)。
///
/// `salt` 为 `None` 时按 RFC 5869 §2.2 取 HashLen 个零字节。`out_len`
/// 上限 255×32([`HKDF_SHA256_MAX_LEN`]),超限属调用方编程错误,显式
/// panic(同 std 切片越界惯例;SSE-C 路径只用常量 32,不会触发)。
///
/// 返回 OKM(`out_len` 字节);内部 PRK 与 expand 链状态用完即擦除。
pub fn hkdf_sha256(ikm: &[u8], salt: Option<&[u8]>, info: &[u8], out_len: usize) -> Vec<u8> {
    assert!(
        out_len <= HKDF_SHA256_MAX_LEN,
        "hkdf-sha256 output length {out_len} exceeds 255*32"
    );
    // extract:PRK = HMAC-Hash(salt, IKM)
    let zero_salt = [0u8; 32];
    let salt = salt.unwrap_or(&zero_salt);
    let mut mac = <HmacSha256 as Mac>::new_from_slice(salt).expect("hmac accepts any key length");
    mac.update(ikm);
    let mut prk = [0u8; 32];
    prk.copy_from_slice(&mac.finalize().into_bytes());

    // expand:T(0) = 空;T(i) = HMAC-Hash(PRK, T(i-1) ‖ info ‖ byte(i))
    let n = out_len.div_ceil(32); // ≤ 255(上限已校验),i as u8 不截断
    let mut okm = Vec::with_capacity(out_len);
    let mut t = [0u8; 32];
    let mut t_len = 0usize;
    for i in 1..=n {
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(&prk).expect("hmac accepts any key length");
        mac.update(&t[..t_len]);
        mac.update(info);
        mac.update(&[i as u8]);
        t.copy_from_slice(&mac.finalize().into_bytes());
        t_len = 32;
        let take = (out_len - okm.len()).min(32);
        okm.extend_from_slice(&t[..take]);
    }
    // 密钥材料归零(OKM 由调用方持有,不在此擦除)
    t.zeroize();
    prk.zeroize();
    okm
}

/// SSE-C 客户密钥(32B AES-256;持有期驻留内存,Drop 时 zeroize 擦除)。
///
/// 客户密钥零落盘、不进审计/日志(ADR-12 DE1);[`SseCKey::data_key`]
/// 每次调用重新派生(HKDF 32B 输出开销可忽略),不缓存 data_key,避免
/// 多一份密钥材料驻留。
pub struct SseCKey {
    key: [u8; 32],
}

impl SseCKey {
    /// 从字节构造(严格 32B,否则 [`SseError::InvalidKeyLength`])。
    pub fn from_bytes(key: &[u8]) -> Result<Self, SseError> {
        let key: &[u8; 32] = key
            .try_into()
            .map_err(|_| SseError::InvalidKeyLength(key.len()))?;
        Ok(Self { key: *key })
    }

    /// 派生 data_key:`HKDF-SHA256(customer_key, salt=None,
    /// info="fasts3-sse-c-v1", 32)`(ADR-12 DE1)。
    pub fn data_key(&self) -> [u8; 32] {
        let okm = hkdf_sha256(&self.key, None, SSE_C_HKDF_INFO, 32);
        okm.try_into().expect("hkdf output length is 32")
    }

    /// 客户密钥 MD5(ADR-12 D-E5 校验子:= 请求头
    /// `x-amz-server-side-encryption-customer-key-md5` 的解码值,协议层
    /// 解析时已验证两者一致)。写路径随 `SseInfo.key_md5` 落盘,读路径
    /// 比对错 key → 400;密钥本体零落盘红线不破(MD5 单向,且该值本
    /// 就随请求明文传输)。
    pub fn key_md5(&self) -> [u8; 16] {
        md5::Md5::digest(self.key).into()
    }
}

impl Drop for SseCKey {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

impl std::fmt::Debug for SseCKey {
    /// 不输出密钥材料(防日志泄漏)。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SseCKey(..)")
    }
}

/// 分片 nonce_base 派生域分隔串(ADR-12 D-E6;与对象级随机 nonce_base
/// 的语义区分,防跨域混淆)。
const PART_NONCE_DOMAIN: &[u8] = b"fasts3-sse-c-part";

/// multipart 分片 nonce_base 确定性派生(ADR-12 D-E6;UploadPart /
/// UploadPartCopy 加密写共用):
///
/// `nonce_base = HMAC-SHA256(data_key, "fasts3-sse-c-part" ‖ upload_id ‖
/// be32(part_number))` 取前 12B。
///
/// **为什么确定性**:同 (upload_id, part_number) 重传 ⇒ 同 nonce_base ⇒
/// 同明文得同密文 ⇒ part ETag(密文 MD5,DE2)稳定,Complete 不因
/// InvalidPart 失败(s3-tests `test_multipart_sse_c_get_part` 重传语义);
/// upload_id 全局唯一 ⇒ 跨上传/跨 part 不复用。
///
/// **安全取舍(写死,勿"修复"为随机)**:GCM nonce 复用条件 = 同
/// data_key + 同 nonce_base + 同 chunk_no 加密**不同**明文。同内容重传
/// 密文逐字节相同,零新信息泄漏;**同 part 以不同内容重传**则前 12B
/// nonce 复用加密不同明文——泄漏两明文异或并削弱该 part 的认证。取舍
/// 依据:① AWS 同语义(重传 = 覆盖,服务端不拒绝);② 不同内容重传同
/// part 号在正常客户端流程中罕见(重传源于超时/连接失败,内容相同);
/// ③ 随机 nonce 的替代案直接破坏重传幂等(ETag 漂移 → Complete
/// InvalidPart),是正确性回退而非安全增强。`data_key` 为零化边界外的
/// 派生材料,本函数输出非密钥材料(nonce_base 本就随元数据落盘)。
pub fn derive_part_nonce_base(data_key: &[u8; 32], upload_id: &str, part_number: u32) -> [u8; 12] {
    let mut mac =
        <HmacSha256 as Mac>::new_from_slice(data_key).expect("hmac accepts any key length");
    mac.update(PART_NONCE_DOMAIN);
    mac.update(upload_id.as_bytes());
    mac.update(&part_number.to_be_bytes());
    let out = mac.finalize().into_bytes();
    let mut nonce_base = [0u8; 12];
    nonce_base.copy_from_slice(&out[..12]);
    nonce_base
}

/// 分块 AES-256-GCM 加密器(单对象粒度;构造后逐 chunk 加/解密)。
///
/// nonce = `HMAC-SHA256(data_key, nonce_base ‖ be64(chunk_no))` 前 12B,
/// aad = nonce_base(对应关系见模块文档)。16B tag 与密文分离返回,由
/// 调用方存元数据;密文与明文等长。
pub struct ChunkedGcm {
    /// HKDF 派生的数据密钥(nonce 派生的 HMAC 密钥;Drop 时擦除)。
    data_key: [u8; 32],
    cipher: Aes256Gcm,
    /// 每对象随机 96bit 标识(等价 ADR-12 DE1 的 object_id 绑定)。
    nonce_base: [u8; 12],
}

impl ChunkedGcm {
    /// 以 data_key(SseCKey::data_key 输出)与每对象随机 nonce_base 构造。
    pub fn new(data_key: [u8; 32], nonce_base: [u8; 12]) -> Self {
        let cipher = Aes256Gcm::new_from_slice(&data_key).expect("aes-256 key is 32 bytes");
        Self {
            data_key,
            cipher,
            nonce_base,
        }
    }

    /// 每对象随机 nonce 基址(引擎写路径落 `SseInfo.nonce_base` 用)。
    pub fn nonce_base(&self) -> [u8; 12] {
        self.nonce_base
    }

    /// 派生 chunk nonce:`HMAC-SHA256(data_key, nonce_base ‖
    /// be64(chunk_no))` 取前 12B(模块文档有与 ADR 原文的对应说明)。
    fn derive_nonce(&self, chunk_no: u64) -> [u8; 12] {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.data_key)
            .expect("hmac accepts any key length");
        mac.update(&self.nonce_base);
        mac.update(&chunk_no.to_be_bytes());
        let out = mac.finalize().into_bytes();
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&out[..12]);
        nonce
    }

    /// 加密一个 chunk,返回(密文, 16B tag);密文与明文等长。
    ///
    /// 调用方契约:每 chunk 明文 ≤ [`SSE_CHUNK_SIZE`](最后一块可短);
    /// 同一 (nonce_base, chunk_no) 绝不重复加密。`&mut self` 预留流式
    /// 状态并借借用规则防并发复用,当前实现无内部可变状态。
    pub fn encrypt_chunk(&mut self, chunk_no: u64, plaintext: &[u8]) -> (Vec<u8>, [u8; 16]) {
        debug_assert!(
            plaintext.len() <= SSE_CHUNK_SIZE,
            "chunk plaintext exceeds 64KiB contract"
        );
        let nonce = self.derive_nonce(chunk_no);
        let mut ct = plaintext.to_vec();
        let tag = self
            .cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), &self.nonce_base, &mut ct)
            .expect("aes-gcm in-place encrypt is infallible");
        let mut tag_out = [0u8; 16];
        tag_out.copy_from_slice(&tag);
        (ct, tag_out)
    }

    /// 解密一个 chunk;tag 认证失败(篡改/错位重放/密钥或对象标识不符)
    /// 返回 [`SseError::AuthenticationFailed`],不输出任何明文。
    pub fn decrypt_chunk(
        &self,
        chunk_no: u64,
        ciphertext: &[u8],
        tag: &[u8; 16],
    ) -> Result<Vec<u8>, SseError> {
        let nonce = self.derive_nonce(chunk_no);
        let mut pt = ciphertext.to_vec();
        self.cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(&nonce),
                &self.nonce_base,
                &mut pt,
                Tag::<Aes256Gcm>::from_slice(tag),
            )
            .map_err(|_| SseError::AuthenticationFailed)?;
        Ok(pt)
    }
}

impl Drop for ChunkedGcm {
    fn drop(&mut self) {
        self.data_key.zeroize();
    }
}

// ─────────────────────────── SSE-S3(M11 K1-1,ADR-12 DS1)───────────────────────────
//
// KEK/DEK 两级:KEK 派生自独立持久化种子 `s:sse_kek_seed`(64B 随机,
// fs3-meta 首次需要时生成,**不与 s:key_seed_salt 访问密钥种子混用**);
// 每对象随机 256bit DEK,`wrapped_dek = AES-256-GCM(KEK_current, DEK)`
// (随机 12B nonce,落盘形态 nonce‖ct‖tag = 60B)。红线:seed/KEK/DEK 明文
// 零落盘、零日志、零导出、永不出任何 API;内存持有副本随 Drop zeroize。
//
// 轮换(DS1)= 新 KEK 代 + 后台重包裹 wrapped_dek;**全部历史代 KEK 由
// seed 确定性派生**,旧代对象在重包裹完成前恒可读,重包裹是卫生收敛而
// 非可读性前提。

/// SSE-S3 KEK 派生 info 前缀(DS1 指定字符串;完整 info = 本前缀 ‖
/// be32(gen),gen 从 1 起)。
const SSE_S3_KEK_HKDF_INFO: &[u8] = b"fasts3-sse-s3-kek-v1";

/// SSE-S3 DEK 包裹值长度:12B 随机 nonce ‖ 32B DEK 密文 ‖ 16B GCM tag。
pub const SSE_S3_WRAPPED_DEK_LEN: usize = 12 + 32 + 16;

/// DEK 包裹/解包的 GCM AAD(域分隔:防包裹值被重放到其他 GCM 用途;
/// 非密钥材料,写死勿改——改了等于换格式)。
const SSE_S3_DEK_WRAP_AAD: &[u8] = b"fasts3-sse-s3-dek";

/// KEK 派生(DS1):`KEK(gen) = HKDF-SHA256(seed, salt=None,
/// info="fasts3-sse-s3-kek-v1" ‖ be32(gen), 32)`;gen 从 1 起。
/// 输出为密钥材料:调用方持有副本用完 zeroize(引擎/arm 内局部消化)。
pub fn derive_sse_s3_kek(seed: &[u8; 64], gen: u32) -> [u8; 32] {
    let mut info = [0u8; SSE_S3_KEK_HKDF_INFO.len() + 4];
    info[..SSE_S3_KEK_HKDF_INFO.len()].copy_from_slice(SSE_S3_KEK_HKDF_INFO);
    info[SSE_S3_KEK_HKDF_INFO.len()..].copy_from_slice(&gen.to_be_bytes());
    hkdf_sha256(seed, None, &info, 32)
        .try_into()
        .expect("hkdf output length is 32")
}

/// DEK 包裹:`AES-256-GCM(KEK, DEK)`,随机 12B nonce,输出 nonce‖ct‖tag
/// (60B,落 `SseInfo.wrapped_dek`)。
pub fn sse_s3_wrap_dek(kek: &[u8; 32], dek: &[u8; 32]) -> Result<Vec<u8>, SseError> {
    let cipher = Aes256Gcm::new_from_slice(kek).expect("aes-256 key is 32 bytes");
    let mut nonce = [0u8; 12];
    crate::random_bytes(&mut nonce).map_err(|_| SseError::Rng)?;
    let mut ct = dek.to_vec();
    let tag = cipher
        .encrypt_in_place_detached(Nonce::from_slice(&nonce), SSE_S3_DEK_WRAP_AAD, &mut ct)
        .expect("aes-gcm in-place encrypt is infallible");
    // in-place 加密后 ct 已是密文(明文副本被就地覆盖),无密钥材料驻留
    let mut out = Vec::with_capacity(SSE_S3_WRAPPED_DEK_LEN);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    out.extend_from_slice(&tag);
    debug_assert_eq!(out.len(), SSE_S3_WRAPPED_DEK_LEN);
    Ok(out)
}

/// DEK 解包:长度不符 → [`SseError::MalformedWrappedDek`];tag 认证失败
/// (KEK 代不符/元数据被篡改)→ [`SseError::AuthenticationFailed`],不输出
/// 任何明文。成功输出的 DEK 为密钥材料,调用方用完 zeroize。
pub fn sse_s3_unwrap_dek(kek: &[u8; 32], wrapped: &[u8]) -> Result<[u8; 32], SseError> {
    if wrapped.len() != SSE_S3_WRAPPED_DEK_LEN {
        return Err(SseError::MalformedWrappedDek(wrapped.len()));
    }
    let cipher = Aes256Gcm::new_from_slice(kek).expect("aes-256 key is 32 bytes");
    let mut pt = wrapped[12..12 + 32].to_vec();
    cipher
        .decrypt_in_place_detached(
            Nonce::from_slice(&wrapped[..12]),
            SSE_S3_DEK_WRAP_AAD,
            &mut pt,
            Tag::<Aes256Gcm>::from_slice(&wrapped[12 + 32..]),
        )
        .map_err(|_| SseError::AuthenticationFailed)?;
    let dek: [u8; 32] = pt.try_into().expect("wrapped DEK body is 32 bytes");
    Ok(dek)
}

/// 签发 SSE-S3 写密钥(M11 K1-1 组合原语;引擎唯一签发入口):
/// seed → KEK(gen) → 随机 256bit DEK → 包裹。KEK 局部副本用完即擦;
/// DEK 明文仅随 [`SseS3WriteKey`] 内存持有(Drop zeroize)。
pub fn mint_sse_s3_write_key(seed: &[u8; 64], gen: u32) -> Result<SseS3WriteKey, SseError> {
    let mut kek = derive_sse_s3_kek(seed, gen);
    let mut dek = [0u8; 32];
    crate::random_bytes(&mut dek).map_err(|_| SseError::Rng)?;
    let wrapped = sse_s3_wrap_dek(&kek, &dek);
    kek.zeroize();
    let wrapped = wrapped?;
    Ok(SseS3WriteKey::new(dek, gen, wrapped))
}

/// 按代解包 DEK(组合原语;读路径/轮换重包裹入口):seed → KEK(kek_id)
/// → 解包。KEK 局部副本用完即擦;返回的 DEK 由调用方消化(ChunkedGcm
/// by-value 接管,或显式 zeroize)。
pub fn unwrap_sse_s3_dek(
    seed: &[u8; 64],
    kek_id: u32,
    wrapped: &[u8],
) -> Result<[u8; 32], SseError> {
    let mut kek = derive_sse_s3_kek(seed, kek_id);
    let dek = sse_s3_unwrap_dek(&kek, wrapped);
    kek.zeroize();
    dek
}

/// DEK 换代重包裹(DS1 轮换 + 异代 copy 的元数据级重包裹):旧代解包 →
/// 新代包裹;DEK 明文不出本函数(局部副本用完即擦),数据面零触碰。
pub fn rewrap_sse_s3_dek(
    seed: &[u8; 64],
    from_gen: u32,
    to_gen: u32,
    wrapped: &[u8],
) -> Result<Vec<u8>, SseError> {
    let mut dek = unwrap_sse_s3_dek(seed, from_gen, wrapped)?;
    let mut kek = derive_sse_s3_kek(seed, to_gen);
    let out = sse_s3_wrap_dek(&kek, &dek);
    kek.zeroize();
    dek.zeroize();
    out
}

/// SSE-S3 写路径密钥材料(M11 K1-1):每对象随机 DEK + 当前代 KEK 包裹值。
/// DEK 明文仅内存持有(Drop zeroize),落盘的只有 wrapped_dek(密文)。
pub struct SseS3WriteKey {
    dek: [u8; 32],
    kek_id: u32,
    wrapped_dek: Vec<u8>,
}

impl SseS3WriteKey {
    pub fn new(dek: [u8; 32], kek_id: u32, wrapped_dek: Vec<u8>) -> Self {
        debug_assert_eq!(wrapped_dek.len(), SSE_S3_WRAPPED_DEK_LEN);
        SseS3WriteKey {
            dek,
            kek_id,
            wrapped_dek,
        }
    }

    /// 包裹所用的 KEK 代(落 `SseInfo.kek_id`;轮换重包裹的比对基准)。
    pub fn kek_id(&self) -> u32 {
        self.kek_id
    }

    /// DEK 包裹值(nonce‖ct‖tag 60B;密文,落 `SseInfo.wrapped_dek`)。
    pub fn wrapped_dek(&self) -> &[u8] {
        &self.wrapped_dek
    }

    /// DEK 副本(数据密钥;调用方消化——ChunkedGcm by-value 接管并随
    /// Drop 擦除,本类型自身 Drop 再擦一次驻留副本)。
    pub fn data_key(&self) -> [u8; 32] {
        self.dek
    }
}

impl Drop for SseS3WriteKey {
    fn drop(&mut self) {
        self.dek.zeroize();
    }
}

impl std::fmt::Debug for SseS3WriteKey {
    /// 不输出密钥材料(wrapped_dek 为密文,但也一并隐去,防日志面扩散)。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SseS3WriteKey(kek_id={})", self.kek_id)
    }
}

/// SSE-KMS 写密钥(M20,ADR-29 KR3):mint 产物 —— 本地随机 DEK(明文仅
/// 内存、Drop zeroize)+ transit 密文与 key_name/context(落 SseInfo V2
/// 载荷)。数据面与 SSE-C/S3 同一 ChunkedGcm 网格。
#[derive(Clone)]
pub struct SseKmsWriteKey {
    key_name: String,
    wrapped_dek: String,
    context_binding: String,
    bucket_key_enabled: Option<bool>,
    data_key: [u8; 32],
}

impl SseKmsWriteKey {
    pub fn new(
        key_name: String,
        wrapped_dek: String,
        context_binding: String,
        data_key: [u8; 32],
    ) -> Self {
        SseKmsWriteKey {
            key_name,
            wrapped_dek,
            context_binding,
            bucket_key_enabled: None,
            data_key,
        }
    }

    /// 桶键头落盘值(D1:接受 + 回显 + 落 meta;优化不做)。
    pub fn with_bucket_key_enabled(mut self, enabled: Option<bool>) -> Self {
        self.bucket_key_enabled = enabled;
        self
    }

    pub fn bucket_key_enabled(&self) -> Option<bool> {
        self.bucket_key_enabled
    }

    pub fn key_name(&self) -> &str {
        &self.key_name
    }

    pub fn wrapped_dek(&self) -> &str {
        &self.wrapped_dek
    }

    pub fn context_binding(&self) -> &str {
        &self.context_binding
    }

    pub fn data_key(&self) -> [u8; 32] {
        self.data_key
    }
}

impl Drop for SseKmsWriteKey {
    fn drop(&mut self) {
        self.data_key.zeroize();
    }
}

impl std::fmt::Debug for SseKmsWriteKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 不输出密钥材料(wrapped_dek 属密文但同样不打印——纪律从紧)
        f.write_str("SseKmsWriteKey(..)")
    }
}

/// 写路径 SSE 密钥(M11 K1-1 泛化;原 `Option<&SseCKey>` 的并集表达):
/// - SSE-C:客户密钥(请求期借用,零落盘;HKDF 派生 data_key);
/// - SSE-S3:服务端 KEK 体系签发的每对象 DEK(明文仅内存持有);
/// - SSE-KMS:外置 KMS transit 包裹的每对象 DEK(M20,ADR-29)。
///
/// 三型共用同一 64KiB 分块网格与落盘 `SseInfo` 形态(读路径按 kind 分派
/// 密钥来源,数据面零分叉)。
#[derive(Debug)]
pub enum SseWriteKey<'a> {
    SseC(&'a SseCKey),
    SseS3(&'a SseS3WriteKey),
    SseKms(SseKmsWriteKey),
}

impl SseWriteKey<'_> {
    /// 数据密钥(SSE-C = HKDF(customer_key);SSE-S3 = DEK 本体;
    /// SSE-KMS = mint 的本地 DEK)。
    /// 输出为密钥材料,调用方消化(ChunkedGcm by-value 接管)。
    pub fn data_key(&self) -> [u8; 32] {
        match self {
            SseWriteKey::SseC(k) => k.data_key(),
            SseWriteKey::SseS3(w) => w.data_key(),
            SseWriteKey::SseKms(w) => w.data_key(),
        }
    }

    /// D-E5 校验子(SSE-C = 客户密钥 MD5;SSE-S3/SSE-KMS = 全零约定,DEK
    /// 由服务端/KMS 密钥体系持有,无客户校验子概念)。
    pub fn key_md5(&self) -> [u8; 16] {
        match self {
            SseWriteKey::SseC(k) => k.key_md5(),
            SseWriteKey::SseS3(_) => [0u8; 16],
            SseWriteKey::SseKms(_) => [0u8; 16],
        }
    }

    /// 按类型构造落盘 SseInfo(SSE-C:kek_id=0/wrapped_dek 空;SSE-S3:
    /// kek_id=当前代、wrapped_dek=包裹值;SSE-KMS:V2 载荷)。
    pub fn build_sse_info(
        &self,
        nonce_base: [u8; 12],
        chunk_tags: Vec<[u8; 16]>,
    ) -> crate::types::SseInfo {
        match self {
            SseWriteKey::SseC(k) => {
                crate::types::SseInfo::sse_c(nonce_base, chunk_tags, k.key_md5())
            }
            SseWriteKey::SseS3(w) => crate::types::SseInfo::sse_s3(
                w.kek_id(),
                w.wrapped_dek().to_vec(),
                nonce_base,
                chunk_tags,
            ),
            SseWriteKey::SseKms(w) => crate::types::SseInfo::sse_kms(
                w.key_name(),
                w.wrapped_dek(),
                nonce_base,
                chunk_tags,
                w.context_binding(),
                w.bucket_key_enabled(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- HKDF-SHA256:RFC 5869 Appendix A(HMAC-SHA-256)官方测试向量 ----

    #[test]
    fn hkdf_rfc5869_case_1() {
        // RFC 5869 A.1:SHA-256,ikm = 0x0b×22,salt/info 短,L = 42
        let okm = hkdf_sha256(
            &[0x0b; 22],
            Some(&(0u8..13).collect::<Vec<_>>()),
            &(0xf0u8..=0xf9).collect::<Vec<_>>(),
            42,
        );
        assert_eq!(
            hex::encode(okm),
            "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
        );
    }

    #[test]
    fn hkdf_rfc5869_case_2() {
        // RFC 5869 A.2:SHA-256,ikm/salt/info 各 80B,L = 82
        let ikm: Vec<u8> = (0x00u8..=0x4f).collect();
        let salt: Vec<u8> = (0x60u8..=0xaf).collect();
        let info: Vec<u8> = (0xb0u8..=0xff).collect();
        let okm = hkdf_sha256(&ikm, Some(&salt), &info, 82);
        assert_eq!(
            hex::encode(okm),
            concat!(
                "b11e398dc80327a1c8e7f78c596a49344f012eda2d4efad8a050cc4c19afa97c",
                "59045a99cac7827271cb41c65e590e09da3275600c2f09b8367793a9aca3db71",
                "cc30c58179ec3e87c14c01d5c1f3434f1d87"
            )
        );
    }

    #[test]
    fn hkdf_rfc5869_case_3() {
        // RFC 5869 A.3:SHA-256,salt 缺省(None → 32 个零字节)、info 空,L = 42
        let okm = hkdf_sha256(&[0x0b; 22], None, &[], 42);
        assert_eq!(
            hex::encode(okm),
            "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8"
        );
    }

    #[test]
    fn hkdf_output_limit() {
        assert_eq!(
            hkdf_sha256(b"k", None, b"i", HKDF_SHA256_MAX_LEN).len(),
            8160
        );
    }

    #[test]
    #[should_panic(expected = "hkdf-sha256 output length")]
    fn hkdf_output_too_long() {
        let _ = hkdf_sha256(b"k", None, b"i", HKDF_SHA256_MAX_LEN + 1);
    }

    // ---- AES-256-GCM:官方已知答案(McGrew & Viega "The Galois/Counter
    // Mode of Operation (GCM)" Appendix B;NIST GCM 标准样例同源) ----

    #[test]
    fn gcm_nist_empty_plaintext() {
        // Test Case 15(AES-256):全零 key/IV,空明文/空 AAD
        let cipher = Aes256Gcm::new_from_slice(&[0u8; 32]).unwrap();
        let mut buf = Vec::new();
        let tag = cipher
            .encrypt_in_place_detached(Nonce::from_slice(&[0u8; 12]), &[], &mut buf)
            .unwrap();
        assert!(buf.is_empty());
        assert_eq!(hex::encode(tag), "530f8afbc74536b9a963b4f1c4cb738b");
    }

    #[test]
    fn gcm_nist_single_block() {
        // Test Case 16(AES-256):全零 key/IV,单块全零明文
        let cipher = Aes256Gcm::new_from_slice(&[0u8; 32]).unwrap();
        let mut buf = [0u8; 16];
        let tag = cipher
            .encrypt_in_place_detached(Nonce::from_slice(&[0u8; 12]), &[], &mut buf)
            .unwrap();
        assert_eq!(hex::encode(buf), "cea7403d4d606b6e074ec5d3baf39d18");
        assert_eq!(hex::encode(tag), "d0d1c8a799996bf0265b98b5d48ab919");
    }

    // ---- SSE-C 密钥与分块原语 ----

    #[test]
    fn ssec_key_derivation() {
        assert!(matches!(
            SseCKey::from_bytes(&[0u8; 31]),
            Err(SseError::InvalidKeyLength(31))
        ));
        assert!(matches!(
            SseCKey::from_bytes(&[0u8; 33]),
            Err(SseError::InvalidKeyLength(33))
        ));
        let k = SseCKey::from_bytes(&[0x07; 32]).unwrap();
        let dk = k.data_key();
        assert_ne!(dk, [0x07; 32], "HKDF 输出 ≠ 原始 key");
        assert_eq!(dk, k.data_key(), "派生确定性");
        assert_eq!(
            dk,
            hkdf_sha256(&[0x07; 32], None, b"fasts3-sse-c-v1", 32)[..],
            "info 字符串与 ADR-12 DE1 一致"
        );
    }

    #[test]
    fn chunked_roundtrip() {
        let key = SseCKey::from_bytes(&[0x42; 32]).unwrap();
        let mut enc = ChunkedGcm::new(key.data_key(), [0xAB; 12]);
        // 3 个满 chunk + 100B 尾块
        let plain: Vec<u8> = (0..3 * SSE_CHUNK_SIZE + 100)
            .map(|i| (i % 251) as u8)
            .collect();
        let mut sealed = Vec::new();
        for (no, c) in plain.chunks(SSE_CHUNK_SIZE).enumerate() {
            let (ct, tag) = enc.encrypt_chunk(no as u64, c);
            assert_eq!(ct.len(), c.len(), "密文等长(ADR-12 DE1)");
            sealed.push((ct, tag));
        }
        assert_eq!(sealed.len(), 4);
        // 逐 chunk 解密还原
        let mut back = Vec::new();
        for (no, (ct, tag)) in sealed.iter().enumerate() {
            back.extend_from_slice(&enc.decrypt_chunk(no as u64, ct, tag).unwrap());
        }
        assert_eq!(back, plain);
        // 相同明文、不同 chunk_no → nonce 不同 → 密文不同
        let (c0, _) = enc.encrypt_chunk(0, b"same bytes");
        let (c1, _) = enc.encrypt_chunk(1, b"same bytes");
        assert_ne!(c0, c1);
    }

    #[test]
    fn tamper_detection() {
        let mut enc = ChunkedGcm::new([0x11; 32], [0x22; 12]);
        let (ct, tag) = enc.encrypt_chunk(0, b"hello fasts3 chunk");

        // 改密文 1 字节
        let mut bad_ct = ct.clone();
        bad_ct[0] ^= 1;
        assert!(matches!(
            enc.decrypt_chunk(0, &bad_ct, &tag),
            Err(SseError::AuthenticationFailed)
        ));
        // 改 tag 1 字节
        let mut bad_tag = tag;
        bad_tag[15] ^= 0x80;
        assert!(matches!(
            enc.decrypt_chunk(0, &ct, &bad_tag),
            Err(SseError::AuthenticationFailed)
        ));
        // chunk_no 错位(重放/重排):chunk 1 的密文按 0 解密,反之亦然
        let (ct1, tag1) = enc.encrypt_chunk(1, b"hello fasts3 chunk");
        assert!(enc.decrypt_chunk(0, &ct1, &tag1).is_err());
        assert!(enc.decrypt_chunk(1, &ct, &tag).is_err());
        // 跨对象重放(nonce_base 不同,aad/nonce 双绑定)
        let other = ChunkedGcm::new([0x11; 32], [0x33; 12]);
        assert!(other.decrypt_chunk(0, &ct, &tag).is_err());
        // 错误 data_key
        let wrong = ChunkedGcm::new([0x99; 32], [0x22; 12]);
        assert!(wrong.decrypt_chunk(0, &ct, &tag).is_err());
    }

    #[test]
    fn nonce_uniqueness() {
        let g = ChunkedGcm::new([0x11; 32], [0x22; 12]);
        // 不同 chunk_no → nonce 不同
        assert_ne!(g.derive_nonce(0), g.derive_nonce(1));
        // 不同 nonce_base → nonce 不同
        let g2 = ChunkedGcm::new([0x11; 32], [0x23; 12]);
        assert_ne!(g.derive_nonce(0), g2.derive_nonce(0));
        // 批量判重(0..1024 无碰撞)
        let mut seen = std::collections::HashSet::new();
        for i in 0..1024u64 {
            assert!(seen.insert(g.derive_nonce(i)), "chunk {i} nonce 碰撞");
        }
    }

    // ---- ADR-12 D-E5/D-E6:校验子与分片 nonce 派生 ----

    #[test]
    fn key_md5_matches_header_semantics() {
        // D-E5:key_md5 = 客户密钥 MD5(= 请求头 customer-key-md5 解码值)
        let k = SseCKey::from_bytes(&[0x42; 32]).unwrap();
        assert_eq!(k.key_md5(), md5::Md5::digest([0x42; 32]).as_slice());
        // 不同密钥 → 不同校验子
        let k2 = SseCKey::from_bytes(&[0x43; 32]).unwrap();
        assert_ne!(k.key_md5(), k2.key_md5());
    }

    #[test]
    fn part_nonce_base_derivation() {
        let k = SseCKey::from_bytes(&[0x5A; 32]).unwrap();
        let dk = k.data_key();
        // D-E6:确定性——同 (data_key, upload_id, part_number) 恒同值
        let n1 = derive_part_nonce_base(&dk, "up-1", 2);
        assert_eq!(n1, derive_part_nonce_base(&dk, "up-1", 2));
        // 跨 part / 跨 upload / 跨密钥互异(nonce 复用面只剩同 part 重传)
        assert_ne!(n1, derive_part_nonce_base(&dk, "up-1", 1));
        assert_ne!(n1, derive_part_nonce_base(&dk, "up-1", 3));
        assert_ne!(n1, derive_part_nonce_base(&dk, "up-2", 2));
        let dk2 = SseCKey::from_bytes(&[0x5B; 32]).unwrap().data_key();
        assert_ne!(n1, derive_part_nonce_base(&dk2, "up-1", 2));
        // 派生值作为 nonce_base 直接可用(加密往返)
        let mut enc = ChunkedGcm::new(dk, n1);
        let (ct, tag) = enc.encrypt_chunk(0, b"part bytes");
        assert_eq!(enc.decrypt_chunk(0, &ct, &tag).unwrap(), b"part bytes");
        // 同 part 重传同明文 ⇒ 同 nonce 同密文(重传幂等的密码学基础)
        let mut enc2 = ChunkedGcm::new(dk, derive_part_nonce_base(&dk, "up-1", 2));
        let (ct2, tag2) = enc2.encrypt_chunk(0, b"part bytes");
        assert_eq!((ct, tag), (ct2, tag2));
    }

    // ---- M11 K1-1:SSE-S3 KEK/DEK 两级(ADR-12 DS1) ----

    #[test]
    fn sse_s3_kek_derivation() {
        let seed = [0xA5u8; 64];
        let k1 = derive_sse_s3_kek(&seed, 1);
        // 确定性
        assert_eq!(k1, derive_sse_s3_kek(&seed, 1));
        // 代间互异(info 含 be32(gen))
        assert_ne!(k1, derive_sse_s3_kek(&seed, 2));
        // 种子互异 → KEK 互异(与 key_seed_salt 域独立的语义基础)
        assert_ne!(k1, derive_sse_s3_kek(&[0x5Au8; 64], 1));
        // info 串与 DS1 写死口径一致
        assert_eq!(
            k1,
            hkdf_sha256(&seed, None, b"fasts3-sse-s3-kek-v1\x00\x00\x00\x01", 32)[..]
        );
    }

    #[test]
    fn sse_s3_dek_wrap_unwrap_roundtrip() {
        let seed = [0xA5u8; 64];
        let kek1 = derive_sse_s3_kek(&seed, 1);
        let kek2 = derive_sse_s3_kek(&seed, 2);
        let dek = [0x11u8; 32];
        let w = sse_s3_wrap_dek(&kek1, &dek).unwrap();
        assert_eq!(w.len(), SSE_S3_WRAPPED_DEK_LEN, "nonce‖ct‖tag = 60B");
        // 同 KEK 解包往返
        assert_eq!(sse_s3_unwrap_dek(&kek1, &w).unwrap(), dek);
        // 随机 nonce:两次包裹同 DEK 密文不同
        let w2 = sse_s3_wrap_dek(&kek1, &dek).unwrap();
        assert_ne!(w, w2);
        assert_eq!(sse_s3_unwrap_dek(&kek1, &w2).unwrap(), dek);
        // 异代 KEK 解包 → 认证失败(轮换语义:旧代包裹值新代不可开)
        assert!(matches!(
            sse_s3_unwrap_dek(&kek2, &w),
            Err(SseError::AuthenticationFailed)
        ));
        // 长度不符 → MalformedWrappedDek(元数据损坏显式)
        assert!(matches!(
            sse_s3_unwrap_dek(&kek1, &w[..40]),
            Err(SseError::MalformedWrappedDek(40))
        ));
        // 篡改 1 字节 → 认证失败
        let mut bad = w.clone();
        bad[20] ^= 1;
        assert!(sse_s3_unwrap_dek(&kek1, &bad).is_err());
    }

    #[test]
    fn sse_write_key_dispatch() {
        let ckey = SseCKey::from_bytes(&[0x42; 32]).unwrap();
        let wk_c = SseWriteKey::SseC(&ckey);
        assert_eq!(wk_c.data_key(), ckey.data_key());
        assert_eq!(wk_c.key_md5(), ckey.key_md5());
        let info = wk_c.build_sse_info([0x01; 12], vec![[0x02; 16]]);
        assert_eq!(info.kind, crate::types::SseKind::SseC);
        assert_eq!(info.kek_id, 0);
        assert!(info.wrapped_dek.is_empty());
        assert_eq!(info.key_md5, ckey.key_md5());

        let seed = [0xA5u8; 64];
        let kek = derive_sse_s3_kek(&seed, 3);
        let dek = [0x77u8; 32];
        let wrapped = sse_s3_wrap_dek(&kek, &dek).unwrap();
        let s3key = SseS3WriteKey::new(dek, 3, wrapped.clone());
        let wk_s3 = SseWriteKey::SseS3(&s3key);
        assert_eq!(wk_s3.data_key(), dek, "SSE-S3 data_key = DEK 本体");
        assert_eq!(wk_s3.key_md5(), [0u8; 16], "SSE-S3 校验子恒零(D-E5)");
        let info = wk_s3.build_sse_info([0x03; 12], vec![[0x04; 16]]);
        assert_eq!(info.kind, crate::types::SseKind::SseS3);
        assert_eq!(info.kek_id, 3);
        assert_eq!(info.wrapped_dek, wrapped);
        assert_eq!(info.key_md5, [0u8; 16]);
        // Debug 不泄漏 DEK/包裹值
        let dbg = format!("{s3key:?}");
        assert!(!dbg.contains("77") && dbg.contains("kek_id=3"), "{dbg}");

        // M20 C1(ADR-29 KR3/KR4):SseKms 三分派全臂
        let dek_k = [0x11u8; 32];
        let kmskey = SseKmsWriteKey::new(
            "fasts3-default".into(),
            "vault:v1:AAAA".into(),
            "fasts3-ssekms-v1\u{1f}b\u{1f}k\u{1f}aws:kms".into(),
            dek_k,
        );
        let wk_k = SseWriteKey::SseKms(kmskey);
        assert_eq!(wk_k.data_key(), dek_k, "SSE-KMS data_key = mint DEK");
        assert_eq!(wk_k.key_md5(), [0u8; 16], "SSE-KMS 校验子恒零");
        let info = wk_k.build_sse_info([0x05; 12], vec![[0x06; 16]]);
        assert_eq!(info.kind, crate::types::SseKind::SseKms);
        assert_eq!(info.kek_id, 0);
        assert_eq!(info.key_md5, [0u8; 16]);
        // V2 载荷首字节 = 版本字节 0x02;解包回三字段
        assert_eq!(info.wrapped_dek[0], crate::types::SSE_KMS_DEK_VERSION);
        let parts = info.kms_parts().unwrap();
        assert_eq!(parts.key_name, "fasts3-default");
        assert_eq!(parts.ciphertext, "vault:v1:AAAA");
        assert!(parts.context_binding.starts_with("fasts3-ssekms-v1"));
        // Debug 不泄漏
        let dbg = format!("{wk_k:?}");
        assert!(!dbg.contains("AAAA"), "{dbg}");
    }

    /// C2(M20;ADR-29 KR3.1/KR6.4):三型密钥共用同一 64KiB ChunkedGcm
    /// 网格——SSE-KMS 与 SSE-C/SSE-S3 的密文/Tag/CRC 语义零分叉。
    #[test]
    fn ssekms_chunked_gcm_roundtrip() {
        let dek = [0x5Au8; 32];
        let wk = SseWriteKey::SseKms(SseKmsWriteKey::new(
            "fasts3-default".into(),
            "vault:v1:Q0lQSEVS".into(),
            "ctx".into(),
            dek,
        ));
        let nonce_base = [0x0Fu8; 12];

        // 网格口径:64KiB×2 + 尾部半 chunk(= 3 tag,ceil 语义)
        let plain = vec![0xCDu8; SSE_CHUNK_SIZE * 2 + 1024];
        let mut cipher = ChunkedGcm::new(wk.data_key(), nonce_base);
        let mut tags = Vec::new();
        let mut ct_all = Vec::new();
        for (no, chunk) in plain.chunks(SSE_CHUNK_SIZE).enumerate() {
            let (ct, tag) = cipher.encrypt_chunk(no as u64, chunk);
            ct_all.extend_from_slice(&ct);
            tags.push(tag);
        }
        assert_eq!(tags.len(), 3, "ceil(size/64KiB) = 3 tag(尾部半 chunk 亦有)");

        // 落盘 info 与读路径解密:同一 data_key + nonce_base 逐 chunk 验 tag
        let info = wk.build_sse_info(nonce_base, tags);
        assert_eq!(info.kind, crate::types::SseKind::SseKms);
        let parts = info.kms_parts().unwrap();
        assert_eq!(parts.ciphertext, "vault:v1:Q0lQSEVS");
        let dec = ChunkedGcm::new(wk.data_key(), info.nonce_base);
        let mut plain_back = Vec::with_capacity(plain.len());
        for (no, chunk) in ct_all.chunks(SSE_CHUNK_SIZE).enumerate() {
            let pt = dec
                .decrypt_chunk(no as u64, chunk, &info.chunk_tags[no])
                .expect("chunk decrypt");
            plain_back.extend_from_slice(&pt);
        }
        assert_eq!(plain_back, plain);
        // 篡改任一 tag → 认证失败(网格语义不变)
        let mut bad_tags = info.chunk_tags.clone();
        bad_tags[1][0] ^= 1;
        let dec = ChunkedGcm::new(wk.data_key(), info.nonce_base);
        let mid = &ct_all[SSE_CHUNK_SIZE..SSE_CHUNK_SIZE * 2];
        assert!(matches!(
            dec.decrypt_chunk(1, mid, &bad_tags[1]),
            Err(SseError::AuthenticationFailed)
        ));
    }
}
