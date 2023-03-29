import type { VersionContext } from '@comunica/types-versioning';
import type { BufferedIteratorOptions } from 'asynciterator';
import { BufferedIterator } from 'asynciterator';
import { BufferedOstrichStore } from 'ostrich-bindings';
import type { OstrichStore } from 'ostrich-bindings';
import { DataFactory } from 'rdf-data-factory';
import type * as RDF from 'rdf-js';

export class OstrichIterator extends BufferedIterator<RDF.Quad> {
  protected readonly store: OstrichStore;
  protected readonly versionContext: VersionContext;
  protected readonly subject?: RDF.Term;
  protected readonly predicate?: RDF.Term;
  protected readonly object?: RDF.Term;

  protected readonly factory: RDF.DataFactory;

  protected reading: boolean;

  public constructor(
    store: OstrichStore,
    versionContext: VersionContext,
    subject?: RDF.Term,
    predicate?: RDF.Term,
    object?: RDF.Term,
    options?: BufferedIteratorOptions,
  ) {
    super(options);
    this.store = store;
    this.versionContext = versionContext;
    this.subject = subject;
    this.predicate = predicate;
    this.object = object;

    this.factory = new DataFactory();

    this.reading = false;
  }

  public _read(count: number, done: () => void): void {
    if (this.store.closed) {
      this.close();
      return done();
    }
    if (this.reading) {
      return done();
    }
    this.reading = true;

    switch (this.versionContext.type) {
      case 'version-materialization':
        this.store.searchTriplesVersionMaterialized(
          this.subject,
          this.predicate,
          this.object,
          { version: this.versionContext.version },
        )
          .then(({ triples }) => {
            triples.forEach(t => this._push(t));
            this.reading = false;
            this.close();
            done();
          })
          .catch(error => this.destroy(error));
        break;
      case 'delta-materialization':
        // eslint-disable-next-line no-case-declarations
        const queryAdditions = this.versionContext.queryAdditions;
        this.store.searchTriplesDeltaMaterialized(
          this.subject,
          this.predicate,
          this.object,
          { versionEnd: this.versionContext.versionEnd, versionStart: this.versionContext.versionStart },
        )
          .then(({ triples }) => {
            triples = triples.filter(t => (<any> t).addition === queryAdditions);
            triples.forEach(t => this._push(t));
            this.reading = false;
            this.close();
            done();
          })
          .catch(error => this.destroy(error));
        break;
      case 'version-query':
        this.store.searchTriplesVersion(this.subject, this.predicate, this.object)
          .then(({ triples }) => {
            triples.forEach(t => {
              t.versions.forEach(version => {
                const quad: RDF.Quad = this.factory.quad(
                  t.subject,
                  t.predicate,
                  t.object,
                  this.factory.namedNode(`version:${version}`),
                );
                this._push(quad);
              });
            });
            this.reading = false;
            this.close();
            done();
          })
          .catch(error => this.destroy(error));
        break;
    }
  }
}

export class BufferedOstrichIterator extends BufferedIterator<RDF.Quad> {
  protected readonly store: BufferedOstrichStore;
  protected queryIterator: IterableIterator<RDF.Quad>;
  protected readonly versionContext: VersionContext;
  protected readonly subject?: RDF.Term;
  protected readonly predicate?: RDF.Term;
  protected readonly object?: RDF.Term;

  protected readonly factory: RDF.DataFactory;

  protected reading: boolean;

  public constructor(
    store: BufferedOstrichStore,
    versionContext: VersionContext,
    subject?: RDF.Term,
    predicate?: RDF.Term,
    object?: RDF.Term,
    options?: BufferedIteratorOptions,
  ) {
    super(options);
    this.store = store;
    this.versionContext = versionContext;
    this.subject = subject;
    this.predicate = predicate;
    this.object = object;

    this.factory = new DataFactory();

    this.reading = false;
  }

  private initializeIterator(): void {
    switch (this.versionContext.type) {
      case 'version-materialization':
        this.queryIterator = this.store.searchTriplesVersionMaterialized(this.subject,
          this.predicate,
          this.object,
          { version: this.versionContext.version });
        break;
      case 'delta-materialization':
        this.queryIterator = this.store.searchTriplesDeltaMaterialized(this.subject,
          this.predicate,
          this.object,
          { versionStart: this.versionContext.versionStart, versionEnd: this.versionContext.versionEnd });
        break;
      case 'version-query':
        this.queryIterator = this.store.searchTriplesVersion(this.subject, this.predicate, this.object);
        break;
    }
  }

  public _read(count: number, done: () => void): void {
    if (this.reading) {
      return done();
    }
    this.reading = true;
    let processed = 0;
    if (typeof this.queryIterator === 'undefined') {
      this.initializeIterator();
    }
    for (const quad of this.queryIterator) {
      switch (this.versionContext.type) {
        case 'delta-materialization':
          // @ts-expect-error When running DM queries, quad will have an "addition" property
          if (quad.addition === this.versionContext.queryAdditions) {
            this._push(quad);
            processed++;
          }
          break;
        case 'version-query':
          // @ts-expect-error When running V queries, quad will have a "versions" property
          for (const version of quad.versions) {
            const quadV: RDF.Quad = this.factory.quad(
              quad.subject,
              quad.predicate,
              quad.object,
              this.factory.namedNode(`version:${version}`),
            );
            this._push(quadV);
            processed++;
          }
          break;
        case 'version-materialization':
          this._push(quad);
          processed++;
          break;
      }
      if (processed === count) {
        break;
      }
    }
    done();
    this.reading = false;
    // If the number of processed quads is inferior to 'count', then the iterator is 'done'
    if (processed < count) {
      this.close();
    }
  }
}

export function makeIterator(store: OstrichStore | BufferedOstrichStore,
  versionContext: VersionContext,
  subject?: RDF.Term,
  predicate?: RDF.Term,
  object?: RDF.Term,
  options?: BufferedIteratorOptions): BufferedIterator<RDF.Quad> {
  if (store instanceof BufferedOstrichStore) {
    return new BufferedOstrichIterator(store, versionContext, subject, predicate, object, options);
  }
  return new OstrichIterator(store, versionContext, subject, predicate, object, options);
}
