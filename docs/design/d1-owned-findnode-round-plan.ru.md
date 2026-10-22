# Private historical owned-round plan / SOURCE-only

Privatnyj neispolzuemyj uzel gotovit istoricheskij poryadok togo zhe owner i claim:512 strok,257 cells; LIVE->BUSY, coordinator.mu->pending.mu, snimok, unlock, fixed order, vozvrat LIVE. Plan ne vydaet admission i ne menyaet accounting, pending phase/priority, round/open/cursor/RR, clock ili debt. Release ochishchaet pointer do CLOSED i borrowed=false. Kopii ticket ostayutsya na starom claim.

Poryadok sokhranyaet L1 cursor, L2 opened/ConnID i po odnomu hodu frozen cell s Session rotation. Fizicheskie overflow gruppy ne poluchayut otdelnye cells. q ostayotsya istoricheskim; zdes net R ili f. C3a kod bez izmenenij. Budushchij driver trebuyet full snapshot revalidation, exact success receipt/same-lock cursor progress i generation-exact refusal cleanup. Receive/T/Ready/writer closure, fizicheskoe primenenie, FORMAT/TYPE/QA/RED i G2 ne zayavleny. Limity/policy A/off defaults ne menyayutsya.
