
pub struct Thing {
    field: String,
}

// pub fn g1(thing: &Thing) -> &String {
//     let tmp = *thing;
// //          ┃ ┗━ Point directly to the referenced data.
// //          ┗━━━ Try to copy RHS's value, otherwise move it into `tmp`.

//     &tmp.field
// }

// Compiles.
fn g2(thing: &Thing) -> &String {
    &(*thing).field
//  ┃ ┗━ Point directly to the referenced data.
//  ┗━━━ Create a reference to the expression's value with a matching lifetime.
}