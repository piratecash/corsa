# Draft: operator migration notes — pin store marker and refusal to start

This is a draft section for the release that ships the secure session. The release workflow publishes
only `RELEASE-v<version>.md`; this file is not published and is merged into that file when the
release is prepared.

## The Node Refuses to Start Over an Unsafe or Lost Pin Store

A node that has proved a peer over the secure session (v2) remembers it in its pin store,
`secure-sessions-<port>.json` next to the peers file (`CORSA_SECURE_SESSION_STORE_PATH` overrides
it). Losing that file, or keeping it where someone else can replace it, would let every such peer be
taken back to the old, weaker session without anyone noticing. From this release the node does not
start in either case, and says why.

**Before upgrading, check the data directory.** The node refuses to start when the pin store, its
marker or any directory above them can be changed by someone other than the node's user or root:

- the file is a symlink, or not a regular file;
- the file is not owned by the node's user with mode `0600`;
- a directory on the way is owned by another user, or is group- or world-writable without the
  sticky bit (`0777`, `0775` and `0770` alike);
- inside a sticky shared directory such as `/tmp`, the next directory is not the node's user's.

The error names the path, the violation with the owner and mode found, the uid and gid the process
runs as, and the fix. The node changes no permissions itself, deletes no pin and does not fall back
to the old session or to a temporary store. Typical fixes: `chmod 0600` the file, `chmod go-w` the
directory, `chown` it to the node's user.

**Docker.** The image runs as `uid 10001` and keeps its data in `/home/corsa/.corsacore`. Earlier
README examples mounted the volume at `/home/corsa/.corsa`, which left the identity and the pin store
in an anonymous volume; mount it at `/home/corsa/.corsacore`. Do not mount a `0777` volume. Either `chown` the volume to
`10001:10001`, or, if other users must write to it, make it sticky (`chmod 1777`), create a
subdirectory owned by `10001` with mode `0700` and point `CORSA_DATA_DIR` at it (README, "Docker").

**The pin store marker.** Next to the identity file the node now keeps
`identity-<port>.json.pin-store-marker.json` (it follows `CORSA_IDENTITY_PATH`). It holds no secret;
it tells a first start from a lost pin store. On the first start of this release a node that already
has a pin store keeps every pin and writes the marker — nothing to do. Afterwards:

- if the pin store is missing (deleted, not restored from a backup, or left behind by a changed
  `CORSA_SECURE_SESSION_STORE_PATH` or data directory), the node refuses to start and names the path
  where the marker last saw it. Restore the file or move it to the path the node now uses. A changed
  port is the same only with an explicit `CORSA_IDENTITY_PATH`;
- if the store was copied rather than moved, so that both the old and the new path hold one, the
  node refuses to start until the other copy is moved aside;
- if the pin store cannot be read, the node refuses to start. Restore it from a backup;
- only if the pin store is lost for good, remove the marker the error names: the node then starts
  with no pins, and every peer may be taken back to the old session once more.

A node that cannot enforce its pins does not start either: a build with the secure session whose
secure session cannot be set up refuses instead of falling back to the old session, and rolling back
to a build without the secure session is refused while a pin store or its marker is on disk.

**A new port is a new node.** With the default paths a port not used before starts a separate node
with a new identity and no pins; when other nodes' markers are in the same directory it warns
(`pin_store_new_node_beside_other_nodes`) and leaves their files alone. Using a port again continues
that port's node with its identity and pins. The protection against a lost pin store is about an
existing node continuing; a new identity carries none of its trust history. To move an existing node
to another port: stop it; rename every file of the old port to the new one (`identity-<port>.json`,
`identity-<port>.json.pin-store-marker.json`, `secure-sessions-<port>.json`, and the other files
carrying the port), or keep them and set `CORSA_IDENTITY_PATH` and `CORSA_SECURE_SESSION_STORE_PATH`
to the old files; start it on the new port (docs/protocol/session_v2.md, "Moving a node to another
port").

**Windows.** The same refusal applies to owners and DACLs: the pin store and its marker must belong to
the node's user (or SYSTEM/Administrators), and no other account may add files to their directory or
delete, rename, re-permission or take ownership of any directory on the way up to the drive's root;
a junction or symlink on the way is refused. Keeping the data in the user's profile (the default)
satisfies this; a directory created directly under `C:\` does not until its permissions are made
private.

When moving a node, copy the pin store together with the identity. An identity restored into an
empty data directory starts as a new node and loses its pins.

On Windows the message appears as a startup notice of the desktop app; elsewhere in the terminal
and in the log of the process that starts the node. It is never returned over RPC.

---

# Черновик: заметки о переходе для операторов — маркер хранилища pin и отказ старта

Это черновик раздела для выпуска, в котором выходит защищённая сессия. Workflow выпуска публикует
только `RELEASE-v<version>.md`; этот файл не публикуется и переносится в тот файл при подготовке
выпуска.

## Узел не стартует при небезопасном или утраченном хранилище pin

Узел, доказавший пира по защищённой сессии (v2), запоминает его в хранилище pin —
`secure-sessions-<port>.json` рядом с файлом пиров (переопределяется
`CORSA_SECURE_SESSION_STORE_PATH`). Утрата этого файла или хранение его там, где его может подменить
кто-то другой, позволили бы незаметно вернуть каждого такого пира к старой, более слабой сессии. С
этого выпуска узел в обоих случаях не стартует и объясняет почему.

**Перед обновлением проверьте каталог данных.** Узел отказывается стартовать, если хранилище pin,
его маркер или любой каталог над ними может изменить кто-то кроме пользователя узла и root:

- файл — symlink или не обычный файл;
- файл не принадлежит пользователю узла или имеет режим не `0600`;
- каталог на пути принадлежит другому пользователю или доступен на запись группе или всем без
  sticky-бита (`0777`, `0775` и `0770` одинаково);
- внутри sticky-каталога общего доступа вроде `/tmp` следующий каталог принадлежит не пользователю
  узла.

Ошибка называет путь, нарушение с найденными владельцем и режимом, uid и gid процесса и способ
исправления. Сам узел права не меняет, pin не удаляет и не переходит ни на старую сессию, ни на
временное хранилище. Типичные исправления: `chmod 0600` для файла, `chmod go-w` для каталога, `chown`
на пользователя узла.

**Docker.** Образ работает от `uid 10001` и хранит данные в `/home/corsa/.corsacore`. Прежние
примеры в README монтировали том в `/home/corsa/.corsa`, из-за чего identity и хранилище pin
оказывались в анонимном томе; монтируйте его в `/home/corsa/.corsacore`. Не монтируйте том `0777`. Либо сделайте `chown` тома на
`10001:10001`, либо, если в том должны писать другие пользователи, сделайте его sticky
(`chmod 1777`), создайте подкаталог, принадлежащий `10001`, с режимом `0700` и укажите на него
`CORSA_DATA_DIR` (README, «Docker»).

**Маркер хранилища pin.** Рядом с файлом identity узел теперь хранит
`identity-<port>.json.pin-store-marker.json` (он следует за `CORSA_IDENTITY_PATH`). Секрета в нём
нет; он отличает первый запуск от утраты хранилища pin. При первом старте этого выпуска узел, у
которого хранилище pin уже есть, сохраняет все pin и записывает маркер — делать ничего не нужно.
Дальше:

- если хранилища pin нет (удалено, не восстановлено из копии или осталось на старом месте после
  смены `CORSA_SECURE_SESSION_STORE_PATH` или каталога данных), узел не стартует и называет путь,
  где маркер видел его последним. Восстановите файл или перенесите его на путь, который узел
  использует теперь. Смена порта ведёт к тому же только при явном `CORSA_IDENTITY_PATH`;
- если хранилище скопировали, а не перенесли, и оно есть и по старому, и по новому пути, узел не
  стартует, пока вторую копию не уберут в сторону;
- если хранилище pin не читается, узел не стартует. Восстановите его из резервной копии;
- только если хранилище pin утрачено безвозвратно, удалите маркер, названный в ошибке: узел
  стартует без pin, и любой пир снова может быть возвращён к старой сессии.

Не стартует и узел, который не может применить свои pin: сборка с защищённой сессией, у которой
защищённую сессию не удалось поднять, отказывает вместо отката на старую сессию, а откат на сборку
без защищённой сессии не стартует, пока на диске есть хранилище pin или его маркер.

**Новый порт — новый узел.** При путях по умолчанию ранее не использованный порт запускает отдельный
узел с новой identity и без pin; если в том же каталоге есть маркеры других узлов, он предупреждает
(`pin_store_new_node_beside_other_nodes`) и их файлы не трогает. Повторное использование порта
продолжает узел этого порта с его identity и pin. Защита от утраты хранилища pin относится к
продолжению работы существующего узла; новая identity его историю доверия не несёт. Чтобы перенести
существующий узел на другой порт: остановите его; переименуйте все файлы прежнего порта на новый
(`identity-<port>.json`, `identity-<port>.json.pin-store-marker.json`, `secure-sessions-<port>.json` и
остальные файлы с портом в имени) или оставьте их и укажите `CORSA_IDENTITY_PATH` и
`CORSA_SECURE_SESSION_STORE_PATH` на прежние файлы; запустите его на новом порту
(docs/protocol/session_v2.md, «Перенос узла на другой порт»).

**Windows.** Тот же отказ действует для владельцев и DACL: хранилище pin и маркер должны принадлежать
пользователю узла (или SYSTEM/Administrators), и никакая другая учётная запись не должна иметь права
добавлять файлы в их каталог или удалять, переименовывать, менять права или забирать владение любым
каталогом на пути до корня диска; junction или symlink на пути отвергается. Данные в профиле
пользователя (по умолчанию) этому удовлетворяют; каталог, созданный прямо под `C:\`, — нет, пока его
права не сделаны закрытыми.

При переносе узла копируйте хранилище pin вместе с identity. Identity, восстановленная в пустой
каталог данных, стартует как новый узел и теряет свои pin.

На Windows сообщение показывается стартовым окном приложения desktop; в остальных случаях — в
терминале и в журнале процесса, запускающего узел. Через RPC оно не возвращается.
