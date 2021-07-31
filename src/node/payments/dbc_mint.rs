
type Payment = BTreeMap<PublicKey, sn_dbc::Dbc>;

/// The management of section funds,
/// via the usage of a distributed AT2 Actor.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub(crate) struct Payments<K: KeyManager> {
    cost: OpCostCalc,
    wallets: RewardWallets,
    mint: Mint<K>,
}

impl<K: KeyManager> Payments<K> {
    ///
    pub(crate) fn new(cost: OpCostCalc, wallets: RewardWallets, mint: Mint<K>) -> Self {
        Self {
            cost,
            wallets,
            mint,
        }
    }
