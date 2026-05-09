# What's New in v1.0.46

## Announce Snapshots Now Collapse to One Winner Per Identity

This release tightens routing announce behavior by collapsing each announce snapshot to a single effective winner per identity.

The practical effect is a cleaner announce plane with less ambiguity about which route variant should represent an identity at snapshot time. That reduces redundant or competing announce entries and makes routing-state publication easier to reason about for both the sender and downstream consumers.

---

# Что нового в v1.0.46

## Announce snapshots теперь схлопываются к одному победителю на identity

В этом релизе tightened routing announce behavior за счёт того, что каждый announce snapshot теперь схлопывается к одному эффективному winner'у на identity.

Практический эффект — более чистый announce plane и меньше неоднозначности в том, какой именно вариант маршрута должен представлять identity на момент построения snapshot. Это уменьшает избыточные или конкурирующие announce entries и делает публикацию routing state проще для понимания как у отправителя, так и у downstream consumers.
