package org.ldg.test


trait FixtureTestHelper {
  type Fixture
  type FixtureConfig

  def mkFixture(cfg: FixtureConfig) : Fixture

  def closeFixture(fixture: Fixture) : Unit = { }

  def withFixture[A](cfg: FixtureConfig)(f: Fixture => A) : A = {
    val fixture = mkFixture(cfg)
    try {
      f(fixture)
    } finally {
      closeFixture(fixture)
    }
  }
}
