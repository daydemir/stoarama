-- Deniz (2026-11-24 01:40Z), verbatim: "just drop the recordings its fine,
-- we'll fix the links whenever". Delivery-page links (delivery_keep_holds) no
-- longer protect R2 objects; Keep Everything and the NAS name+size rule still
-- do. This drops 0190's guard on every R2 reservation and receipt (last
-- redefined in 0225). The NAS-side delivery holds are untouched.
DROP TRIGGER delivery_r2_reservation ON r2_retirement_reservations;
DROP TRIGGER delivery_r2_retirement ON r2_object_retirements;
DROP FUNCTION guard_delivery_r2_retirement();
